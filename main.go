package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"
)

const (
	sldlBinary = "/sldl/sldl"
	musicDir   = "/music"
	mbBase     = "https://musicbrainz.org/ws/2"
	userAgent  = "sldl-wrapper/1.0 (https://github.com/example/sldl-wrapper)"

	// sldl tolerance: 10 min per stalled track instead of sldl's ~50s default.
	// Soulseek users with queues / no slots often deliver eventually.
	sldlMaxStaleTime = "600000"

	// minBitrate is the default hard floor for downloads. Previews search without
	// it so the UI can offer lower-bitrate results when nothing reaches it.
	minBitrate = "320"
)

var errorKeywords = []string{"fail", "error", "no result", "not found", "unable", "warning", "timeout", "connect"}

var losslessExt = map[string]bool{".flac": true, ".wav": true, ".alac": true, ".ape": true, ".wv": true}

var audioExt = map[string]bool{
	".mp3":  true,
	".flac": true,
	".m4a":  true,
	".ogg":  true,
	".opus": true,
	".wav":  true,
	".aac":  true,
}

func isSignificant(line string) bool {
	lower := strings.ToLower(line)
	for _, kw := range errorKeywords {
		if strings.Contains(lower, kw) {
			return true
		}
	}
	return false
}

// --- Job queue ---

type DownloadJob struct {
	cmd       *exec.Cmd
	outDir    string
	startTime time.Time
	output    chan string
	exitErr   chan error
	// onStart, if set, gets the running process (so a download can be cancelled).
	onStart func(*os.Process)
}

// jobQueue runs downloads (and previews, unless a preview account is set) one
// at a time: a Soulseek user can only be logged in once, so a second sldl on
// the same account would kick the first off.
var jobQueue = make(chan *DownloadJob, 50)

// previewQueue is jobQueue unless SLDL_PREVIEW_USER is set, in which case
// previews log in as that account and run alongside downloads.
var previewQueue = jobQueue

// previewListenPort keeps a concurrent preview sldl off the download's
// default listen port (49998), which it couldn't bind.
const previewListenPort = "49997"

func previewCredentials() (user, pass string, separate bool) {
	if u, p := os.Getenv("SLDL_PREVIEW_USER"), os.Getenv("SLDL_PREVIEW_PASS"); u != "" && p != "" {
		return u, p, true
	}
	return os.Getenv("SLDL_USER"), os.Getenv("SLDL_PASS"), false
}

func jobWorker(queue chan *DownloadJob) {
	for job := range queue {
		job.startTime = time.Now()

		pr, pw, err := os.Pipe()
		if err != nil {
			job.exitErr <- err
			close(job.output)
			continue
		}
		job.cmd.Stdout = pw
		job.cmd.Stderr = pw

		if err := job.cmd.Start(); err != nil {
			pw.Close()
			pr.Close()
			job.exitErr <- err
			close(job.output)
			continue
		}
		pw.Close()
		if job.onStart != nil {
			job.onStart(job.cmd.Process)
		}

		scanner := bufio.NewScanner(pr)
		scanner.Buffer(make([]byte, 10*1024*1024), 10*1024*1024)
		for scanner.Scan() {
			job.output <- scanner.Text()
		}
		close(job.output)
		job.exitErr <- job.cmd.Wait()
	}
}

// --- HTTP handlers ---

// queryPunctuation maps MusicBrainz typography to the ASCII that shared file
// names use; Soulseek matches words literally, so "Don’t" won't find "Don't".
// Spaced dashes become spaces so they can't be mistaken for sldl's
// "Artist - Album" separator.
var queryPunctuation = strings.NewReplacer(
	"’", "'", "‘", "'", "“", `"`, "”", `"`,
	" – ", " ", " — ", " ",
	"‐", "-", "‑", "-", "‒", "-", "–", "-", "—", "-",
	"…", " ",
	" & ", " ", "&", " ",
)

// sanitizeQuery normalizes a name search for sldl. slsk:// URLs are returned
// untouched: they are exact user/folder paths (user names can contain "&").
func sanitizeQuery(q string) string {
	if strings.HasPrefix(q, "slsk://") {
		return q
	}
	return strings.Join(strings.Fields(queryPunctuation.Replace(q)), " ")
}

// buildSldlArgs constructs the common sldl invocation. If outDir is empty the
// --path flag is omitted (used by preview, which doesn't write to disk); if
// bitrate is empty no --min-bitrate floor is applied.
func buildSldlArgs(username, password, outDir, query, bitrate string, isAlbum bool, extra ...string) []string {
	args := []string{"--username", username, "--password", password, "--max-stale-time", sldlMaxStaleTime}
	if bitrate != "" {
		args = append(args, "--min-bitrate", bitrate)
	}
	if outDir != "" {
		args = append(args, "--path", outDir)
	}
	args = append(args, extra...)
	if isAlbum {
		args = append(args, "--album")
	}
	args = append(args, sanitizeQuery(query))
	return args
}

// pathContainsFailed reports whether any path segment of rel is "failed" (case-insensitive).
func pathContainsFailed(rel string) bool {
	for _, part := range strings.Split(rel, string(filepath.Separator)) {
		if strings.EqualFold(part, "failed") {
			return true
		}
	}
	return false
}

// findSavedAlbums returns directories under outDir that contain audio files modified
// after startTime and are NOT inside a "failed/" subtree.
func findSavedAlbums(outDir string, startTime time.Time) []string {
	return scanForAudioDirs(outDir, startTime, false)
}

// findFailedAlbums returns directories inside a "failed/" subtree that contain
// audio files modified after startTime.
func findFailedAlbums(outDir string, startTime time.Time) []string {
	return scanForAudioDirs(outDir, startTime, true)
}

func scanForAudioDirs(outDir string, startTime time.Time, wantFailed bool) []string {
	var out []string
	filepath.WalkDir(outDir, func(path string, d os.DirEntry, err error) error {
		if err != nil || !d.IsDir() {
			return nil
		}
		rel, _ := filepath.Rel(outDir, path)
		if rel == "." {
			return nil
		}
		inFailed := pathContainsFailed(rel)
		if wantFailed != inFailed {
			return nil
		}
		entries, err := os.ReadDir(path)
		if err != nil {
			return nil
		}
		for _, e := range entries {
			if e.IsDir() {
				continue
			}
			if !audioExt[strings.ToLower(filepath.Ext(e.Name()))] {
				continue
			}
			info, err := e.Info()
			if err == nil && info.ModTime().After(startTime) {
				out = append(out, path)
				return filepath.SkipDir
			}
		}
		return nil
	})
	return out
}

// removeFailedParents finds the "failed" ancestor of each given path under outDir
// and removes it. Used to clean up sldl's failed/<album>/ residue before retrying.
func removeFailedParents(outDir string, failedAlbums []string, log func(string)) {
	seen := map[string]bool{}
	for _, fdir := range failedAlbums {
		cur := fdir
		for {
			if cur == outDir || cur == "/" || cur == "." {
				break
			}
			if strings.EqualFold(filepath.Base(cur), "failed") {
				if !seen[cur] {
					seen[cur] = true
					if err := os.RemoveAll(cur); err == nil {
						log(fmt.Sprintf("[cleanup] removed %s", cur))
					} else {
						log(fmt.Sprintf("[cleanup] could not remove %s: %v", cur, err))
					}
				}
				break
			}
			cur = filepath.Dir(cur)
		}
	}
}

// extractJSONArray decodes the first JSON array in sldl's mixed output
// (`--print json-all` prints log lines around it) into v.
func extractJSONArray(output string, v any) bool {
	for i, ch := range output {
		if ch == '[' {
			if err := json.Unmarshal([]byte(output[i:]), v); err == nil {
				return true
			}
		}
	}
	return false
}

// slskResult is one folder from sldl's `--print json-all` album output.
type slskResult struct {
	User struct {
		Username          string
		HasFreeUploadSlot bool
		UploadSpeed       float64
	}
	Files []struct {
		Filename string
		Length   int // seconds
		Bitrate  int
		Size     int64
	}
}

var bracketedRe = regexp.MustCompile(`\([^)]*\)|\[[^\]]*\]`)

// normTitle is the key a file name and a tracklist title are compared by:
// bracketed extras ("(feat. X)", "[Remastered]") dropped, then letters and
// digits only. Keep in sync with normTitle in static/index.html.
func normTitle(s string) string {
	s = bracketedRe.ReplaceAllString(strings.ToLower(s), "")
	return strings.Map(func(r rune) rune {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			return r
		}
		return -1
	}, s)
}

// audioFiles returns the result's audio files' names (without folder or
// extension) as title keys, with their lengths and bitrates.
func (r slskResult) audioFiles() (keys []string, lengths, bitrates []int) {
	for _, f := range r.Files {
		name := f.Filename[strings.LastIndexAny(f.Filename, `\/`)+1:]
		if !audioExt[strings.ToLower(filepath.Ext(name))] {
			continue
		}
		key := strings.TrimLeft(normTitle(strings.TrimSuffix(name, filepath.Ext(name))), "0123456789")
		keys = append(keys, key)
		lengths = append(lengths, f.Length)
		bitrates = append(bitrates, f.Bitrate)
	}
	return
}

// countTrackMatches counts the tracks that have a file in r: the file's key
// is, starts with or ends with the title, and the lengths are within 7s when
// both are known. Each file counts once. Keep in sync with countTrackMatches
// in static/index.html.
func countTrackMatches(r slskResult, tracks []ExpectedTrack) int {
	keys, lengths, _ := r.audioFiles()
	used := make([]bool, len(keys))
	matched := 0
	for _, t := range tracks {
		title := normTitle(t.Title)
		if title == "" {
			continue
		}
		want := (t.Length + 500) / 1000
		for i, k := range keys {
			if used[i] || !(k == title || strings.HasPrefix(k, title) || strings.HasSuffix(k, title)) {
				continue
			}
			if want > 0 && lengths[i] > 0 && (lengths[i]-want > 7 || want-lengths[i] > 7) {
				continue
			}
			used[i] = true
			matched++
			break
		}
	}
	return matched
}

// maxCopyFiles is the most audio files a folder may hold and still count as a
// copy of a release with n tracks: room for a deluxe edition's bonus tracks,
// but a 12-song album holding a single's one title is not that single. Keep
// in sync with maxCopyFiles in static/index.html.
func maxCopyFiles(n int) int { return 2*n + 2 }

// fallbackCandidates returns slsk:// URLs of the results that hold every
// track of the tracklist, and at most maxCopyFiles audio files, with no audio
// file below the bitrate floor (unknown bitrate passes, as in sldl), skipping
// skipUser. Lossy (320k) before lossless, then free upload slots, fewest
// extra files, upload speed.
func fallbackCandidates(results []slskResult, tracks []ExpectedTrack, skipUser string) []string {
	floor, _ := strconv.Atoi(minBitrate)
	type cand struct {
		url      string
		r        slskResult
		extra    int
		lossless bool
	}
	var cands []cand
	for _, r := range results {
		if r.User.Username == skipUser || countTrackMatches(r, tracks) < len(tracks) {
			continue
		}
		keys, _, bitrates := r.audioFiles()
		if len(keys) > maxCopyFiles(len(tracks)) {
			continue
		}
		lowRate := false
		for _, b := range bitrates {
			if b > 0 && b < floor {
				lowRate = true
			}
		}
		if lowRate {
			continue
		}
		var dir string
		lossless := false
		for _, f := range r.Files {
			ext := strings.ToLower(filepath.Ext(f.Filename))
			if !audioExt[ext] {
				continue
			}
			if dir == "" {
				dir = strings.TrimLeft(strings.ReplaceAll(f.Filename, `\`, "/"), "/")
				dir = dir[:max(strings.LastIndex(dir, "/"), 0)]
			}
			lossless = lossless || losslessExt[ext]
		}
		cands = append(cands, cand{"slsk://" + r.User.Username + "/" + dir, r, len(keys) - len(tracks), lossless})
	}
	sort.SliceStable(cands, func(i, j int) bool {
		a, b := cands[i], cands[j]
		if a.lossless != b.lossless {
			return !a.lossless
		}
		if a.r.User.HasFreeUploadSlot != b.r.User.HasFreeUploadSlot {
			return a.r.User.HasFreeUploadSlot
		}
		if a.extra != b.extra {
			return a.extra < b.extra
		}
		return a.r.User.UploadSpeed > b.r.User.UploadSpeed
	})
	urls := make([]string, len(cands))
	for i, c := range cands {
		urls[i] = c.url
	}
	return urls
}

func main() {
	http.HandleFunc("/", serveIndex)
	http.HandleFunc("/search", handleSearch)
	http.HandleFunc("/release", handleRelease)
	http.HandleFunc("/release-group", handleReleaseGroup)
	http.HandleFunc("/download", handleDownload)
	http.HandleFunc("/preview", handlePreview)
	http.HandleFunc("/queue", handleQueueStatus)
	http.HandleFunc("/retag", handleRetag)
	http.HandleFunc("/jobs", handleJobs)
	http.HandleFunc("/jobs/log", handleJobLog)
	http.HandleFunc("/jobs/dismiss", handleJobDismiss)
	http.HandleFunc("/jobs/cancel", handleJobCancel)

	go jobWorker(jobQueue)
	if _, _, separate := previewCredentials(); separate {
		previewQueue = make(chan *DownloadJob, 50)
		go jobWorker(previewQueue)
		log.Printf("previews use their own Soulseek account")
	}
	loadJobs()

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	log.Printf("Listening on :%s", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func serveIndex(w http.ResponseWriter, r *http.Request) {
	// Revalidate every load: a tab restored from an old cached page would talk
	// to the current API with a stale client.
	w.Header().Set("Cache-Control", "no-cache")
	http.ServeFile(w, r, "static/index.html")
}

func handleSearch(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query().Get("q")
	kind := r.URL.Query().Get("type")
	if q == "" {
		http.Error(w, "missing q", http.StatusBadRequest)
		return
	}

	var endpoint string
	switch kind {
	case "album":
		endpoint = "/release-group/"
	default:
		endpoint = "/recording/"
	}

	mbURL := fmt.Sprintf("%s%s?query=%s&fmt=json&limit=50", mbBase, endpoint, url.QueryEscape(q))
	req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, mbURL, nil)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	req.Header.Set("User-Agent", userAgent)

	resp, err := mbDo(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	w.Header().Set("Content-Type", "application/json")
	io.Copy(w, resp.Body)
}

// mbDo sends a MusicBrainz request, retrying when MusicBrainz answers 503
// ("server is currently busy", its rate limit), which happens several times
// a day. A response that is still not 200 becomes an error, so it never
// reaches the UI looking like an empty result.
func mbDo(req *http.Request) (*http.Response, error) {
	for attempt := 0; ; attempt++ {
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}
		if resp.StatusCode == http.StatusOK {
			return resp, nil
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusServiceUnavailable || attempt == 3 {
			return nil, fmt.Errorf("MusicBrainz answered %s", resp.Status)
		}
		select {
		case <-req.Context().Done():
			return nil, req.Context().Err()
		case <-time.After(time.Duration(attempt+1) * time.Second):
		}
	}
}

func handleRelease(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	if id == "" {
		http.Error(w, "missing id", http.StatusBadRequest)
		return
	}

	mbURL := fmt.Sprintf("%s/release/%s?inc=recordings&fmt=json", mbBase, url.PathEscape(id))
	req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, mbURL, nil)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	req.Header.Set("User-Agent", userAgent)

	resp, err := mbDo(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	w.Header().Set("Content-Type", "application/json")
	io.Copy(w, resp.Body)
}

func handleReleaseGroup(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	if id == "" {
		http.Error(w, "missing id", http.StatusBadRequest)
		return
	}

	rgURL := fmt.Sprintf("%s/release-group/%s?inc=releases&fmt=json", mbBase, url.PathEscape(id))
	req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, rgURL, nil)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	req.Header.Set("User-Agent", userAgent)
	resp, err := mbDo(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	var rg struct {
		Releases []struct {
			ID     string `json:"id"`
			Date   string `json:"date"`
			Status string `json:"status"`
		} `json:"releases"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&rg); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	picked := ""
	pickedDate := ""
	for _, rel := range rg.Releases {
		if rel.Status != "Official" {
			continue
		}
		if picked == "" || (rel.Date != "" && (pickedDate == "" || rel.Date < pickedDate)) {
			picked = rel.ID
			pickedDate = rel.Date
		}
	}
	if picked == "" {
		for _, rel := range rg.Releases {
			if picked == "" || (rel.Date != "" && (pickedDate == "" || rel.Date < pickedDate)) {
				picked = rel.ID
				pickedDate = rel.Date
			}
		}
	}
	if picked == "" {
		http.Error(w, "no releases found for release group", http.StatusNotFound)
		return
	}

	relURL := fmt.Sprintf("%s/release/%s?inc=recordings&fmt=json", mbBase, url.PathEscape(picked))
	req2, err := http.NewRequestWithContext(r.Context(), http.MethodGet, relURL, nil)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	req2.Header.Set("User-Agent", userAgent)
	resp2, err := mbDo(req2)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	defer resp2.Body.Close()

	w.Header().Set("Content-Type", "application/json")
	io.Copy(w, resp2.Body)
}

func handleQueueStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"queued":%d,"downloads":%d}`, len(jobQueue), downloadsPending.Load())
}

const promoStripScript = `
import sys, glob, os, re
from mutagen.id3 import ID3, TIT2
PROMO_RE = re.compile(r'(?:CORERADIO|CORE\s*RADIO|\.ONLINE)', re.I)
d = sys.argv[1]
count = 0
for f in glob.glob(os.path.join(d, '**', '*.mp3'), recursive=True) + glob.glob(os.path.join(d, '*.mp3')):
    try:
        tags = ID3(f)
        changed = False
        t = tags.get('TIT2')
        if t:
            orig = str(t)
            new = re.sub(r'\s*[\(\[][^\)\]]*(?:RADIO|PROMO|ONLINE)[^\)\]]*[\)\]]', '', orig, flags=re.I).strip()
            if new != orig:
                tags['TIT2'] = TIT2(encoding=3, text=new)
                changed = True
                print('TIT2 fixed: ' + os.path.basename(f) + ': ' + repr(orig) + ' -> ' + repr(new))
        t3 = tags.get('TIT3')
        if t3 and PROMO_RE.search(str(t3)):
            del tags['TIT3']
            changed = True
            print('TIT3 removed: ' + os.path.basename(f) + ': ' + repr(str(t3)))
        tpub = tags.get('TPUB')
        if tpub and PROMO_RE.search(str(tpub)):
            del tags['TPUB']
            changed = True
            print('TPUB removed: ' + os.path.basename(f))
        for key in list(tags.keys()):
            if key.startswith('TXXX'):
                if PROMO_RE.search(str(tags[key])):
                    del tags[key]
                    changed = True
                    print('TXXX removed: ' + os.path.basename(f) + ': ' + key)
        if changed:
            tags.save()
            count += 1
    except Exception as e:
        print('Error ' + str(f) + ': ' + str(e))
print('Stripped promo text from ' + str(count) + ' file(s)')
`

// --- SSE streaming ---

// emitFunc sends one SSE "data:" line to the client.
type emitFunc func(format string, a ...any)

// sseWriteTimeout bounds each write so a phone that silently dropped off the
// network is treated as gone instead of stalling the pipeline until TCP gives up.
const sseWriteTimeout = 30 * time.Second

// serveSSE runs work in its own goroutine and streams what it emits to the
// client. If the client goes away, work keeps running and its output is
// discarded, so closing the tab never kills or stalls a download.
func serveSSE(w http.ResponseWriter, r *http.Request, work func(emit emitFunc)) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	rc := http.NewResponseController(w)

	events := make(chan string, 200)
	gone := make(chan struct{})
	emit := func(format string, a ...any) {
		select {
		case events <- fmt.Sprintf(format, a...):
		case <-gone:
		}
	}
	go func() {
		defer close(events)
		work(emit)
	}()

	defer close(gone)
	for {
		select {
		case msg, ok := <-events:
			if !ok {
				return
			}
			rc.SetWriteDeadline(time.Now().Add(sseWriteTimeout))
			if _, err := fmt.Fprintf(w, "data: %s\n\n", msg); err != nil {
				return
			}
			if err := rc.Flush(); err != nil {
				return
			}
		case <-r.Context().Done():
			return
		}
	}
}

func runPromoStrip(emit emitFunc, dir string) {
	emit("[beets] Stripping promo tags...")

	cmd := exec.Command("python3", "-c", promoStripScript, dir)
	pr, pw, err := os.Pipe()
	if err != nil {
		emit("[beets] promo strip error: %v", err)
		return
	}
	cmd.Stdout = pw
	cmd.Stderr = pw
	if err := cmd.Start(); err != nil {
		pw.Close()
		pr.Close()
		emit("[beets] promo strip error: %v", err)
		return
	}
	pw.Close()
	scanner := bufio.NewScanner(pr)
	for scanner.Scan() {
		emit("[beets] %s", scanner.Text())
	}
	cmd.Wait()
}

// staleTagStripScript removes the MusicBrainz IDs and edition details
// (media, label, junk years like 0001) an uploader's files often carry from a
// release that has since been merged or that isn't the one being matched.
// beets counts every mismatch against a candidate, so with them a perfect
// tracklist match can score 0.10-0.23 and be skipped under strong_rec_thresh.
const staleTagStripScript = `
import sys, glob, os
import mutagen
from mutagen.id3 import ID3
d = sys.argv[1]
ID3_DROP = {'TMED', 'TXXX:MEDIA', 'TPUB', 'TXXX:LABEL', 'TXXX:RELEASECOUNTRY', 'TXXX:BARCODE', 'TXXX:CATALOGNUMBER'}
VORBIS_DROP = {'media', 'label', 'organization', 'releasecountry', 'barcode', 'catalognumber'}
count = 0
for f in glob.glob(os.path.join(glob.escape(d), '**', '*'), recursive=True):
    ext = os.path.splitext(f)[1].lower()
    try:
        if ext == '.mp3':
            audio = tags = ID3(f)
            drop = [k for k in tags.keys() if k.startswith('UFID') or k.startswith('TXXX:MusicBrainz') or k in ID3_DROP
                    or (k == 'TDRC' and str(tags[k])[:4] < '1900')]
        elif ext in ('.flac', '.ogg', '.opus'):
            audio = mutagen.File(f)
            tags = audio.tags
            drop = [k for k in set(tags.keys()) if k.lower().startswith('musicbrainz_') or k.lower() in VORBIS_DROP
                    or (k.lower() == 'date' and tags[k][0][:4] < '1900')]
        else:
            continue
        for k in drop:
            del tags[k]
        if drop:
            audio.save()
            count += 1
    except Exception as e:
        print('Error ' + f + ': ' + str(e))
print('Removed stale MusicBrainz/edition tags from ' + str(count) + ' file(s)')
`

// beetsRetryLine is emitted before the second beets run; Job.beetsLine resets
// the tag outcome on it.
const beetsRetryLine = "Skipped; retrying without the files' own MusicBrainz and edition tags..."

func runBeets(emit emitFunc, dir string) {
	emit("[beets] Auto-tagging with beets...")
	if !beetsImport(emit, dir) {
		return
	}
	emit("[beets] %s", beetsRetryLine)
	out, err := exec.Command("python3", "-c", staleTagStripScript, dir).CombinedOutput()
	for _, l := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		emit("[beets] %s", l)
	}
	if err != nil {
		emit("[beets] finished with warning: tag cleanup failed: %v", err)
		return
	}
	beetsImport(emit, dir)
}

// beetsImport runs one quiet beets import of dir and reports whether beets
// skipped it.
func beetsImport(emit emitFunc, dir string) (skipped bool) {
	// -q: never prompt; apply confident MusicBrainz matches, otherwise fall
	// back to import.quiet_fallback. (-A would be --noautotag.)
	cmd := exec.Command("beet", "--config=/etc/beets/config.yaml", "import", "-q", dir)
	pr, pw, err := os.Pipe()
	if err != nil {
		emit("[beets] Could not start beets: %v", err)
		return false
	}
	cmd.Stdout = pw
	cmd.Stderr = pw
	if err := cmd.Start(); err != nil {
		pw.Close()
		pr.Close()
		emit("[beets] Could not start beets: %v", err)
		return false
	}
	pw.Close()

	scanner := bufio.NewScanner(pr)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.TrimSpace(line) == "Skipping." {
			skipped = true
		}
		emit("[beets] %s", line)
	}
	if err := cmd.Wait(); err != nil {
		emit("[beets] finished with warning: %v", err)
	} else {
		emit("[beets] tagging complete")
	}
	return skipped
}

func handleRetag(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Query().Get("path")
	if path == "" {
		http.Error(w, "missing path", http.StatusBadRequest)
		return
	}

	clean := filepath.Clean(path)
	if !strings.HasPrefix(clean, musicDir) {
		http.Error(w, "invalid path", http.StatusBadRequest)
		return
	}

	serveSSE(w, r, func(emit emitFunc) {
		runPromoStrip(emit, clean)
		runBeets(emit, clean)
		emit("[done]")
	})
}

type DownloadRequest struct {
	Query     string `json:"query"`
	Album     bool   `json:"album"`
	Subfolder string `json:"subfolder"`
	// LowBitrate drops the minBitrate floor, for when the user picked a
	// below-320 result because nothing better was available.
	LowBitrate bool `json:"lowBitrate"`
	// Label is the human name ("Artist – Album") shown in the downloads panel.
	Label string `json:"label"`
	// FallbackQuery ("Artist - Album") is searched when a slsk:// source
	// fails. For an album, Tracks is the MusicBrainz tracklist a replacement
	// must fully contain; without it no other source is tried.
	FallbackQuery string          `json:"fallbackQuery"`
	Tracks        []ExpectedTrack `json:"tracks"`
	// Alternatives are slsk:// URLs of other full copies the preview found,
	// best first. The album fallback tries them before searching again: a
	// repeat of the same search minutes later gets far fewer responses.
	Alternatives []string `json:"alternatives"`
	// Expect is how many of Tracks the picked folder holds. Below len(Tracks)
	// the user knowingly picked an incomplete copy, so that many is enough.
	Expect int `json:"expect"`
}

type ExpectedTrack struct {
	Title  string `json:"title"`
	Length int    `json:"length"` // ms, 0 if unknown
}

// downloadMu serializes whole download pipelines (sldl, fallback retry, beets).
// findSavedAlbums finds a download's album by file mtimes, so the next sldl run
// must not start while beets is still rewriting the previous album's tags, or
// that album would be picked up (and the new one missed).
var (
	downloadMu       sync.Mutex
	downloadsPending atomic.Int32
)

func handlePreview(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req DownloadRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if req.Query == "" {
		http.Error(w, "missing query", http.StatusBadRequest)
		return
	}

	if slskdURL != "" {
		previewSlskd(w, r, req.Query)
		return
	}

	username, password, separate := previewCredentials()
	if username == "" || password == "" {
		http.Error(w, "SLDL_USER or SLDL_PASS not set", http.StatusInternalServerError)
		return
	}

	extra := []string{"--print", "json-all"}
	if separate {
		extra = append(extra, "--listen-port", previewListenPort)
	}
	args := buildSldlArgs(username, password, "", req.Query, "", req.Album, extra...)

	// A preview is only useful to the client that asked, so it dies with the request.
	job := &DownloadJob{
		cmd:     exec.CommandContext(r.Context(), sldlBinary, args...),
		outDir:  musicDir,
		output:  make(chan string, 200),
		exitErr: make(chan error, 1),
	}

	serveSSE(w, r, func(emit emitFunc) {
		if separate {
			if n := len(previewQueue); n > 0 {
				emit("[queued] %d search(es) ahead, waiting…", n)
			}
		} else if len(jobQueue) > 0 || downloadsPending.Load() > 0 {
			emit("[queued] Waiting for the current download to finish…")
		}

		previewQueue <- job

		var allLines, sigLines []string
		for line := range job.output {
			allLines = append(allLines, line)
			if isSignificant(line) {
				sigLines = append(sigLines, line)
			}
			emit("[log] %s", line)
		}
		exitErr := <-job.exitErr

		fullOutput := strings.Join(allLines, "\n")
		var results []json.RawMessage
		jsonFound := extractJSONArray(fullOutput, &results)

		// sldl prints nothing at all when a search comes back empty, so blank
		// output with a clean exit is a genuine "nobody shares this".
		switch {
		case jsonFound:
			log.Printf("preview %q: %d result(s)", req.Query, len(results))
			compact, _ := json.Marshal(results)
			emit("[results] %s", compact)
		case exitErr != nil:
			log.Printf("preview %q failed: %v %q", req.Query, exitErr, sigLines)
			emit("[preview-error] sldl exited: %v", exitErr)
			for _, l := range sigLines {
				emit("[preview-error] %s", l)
			}
		case strings.TrimSpace(fullOutput) == "":
			log.Printf("preview %q: no results", req.Query)
			emit("[results] []")
		default:
			log.Printf("preview %q: unparseable output %q", req.Query, sigLines)
			rawJSON, _ := json.Marshal(fullOutput)
			emit("[results-raw] %s", rawJSON)
		}
		emit("[preview-done]")
	})
}

// searchSlskdOrAlbum is searchSlskd, retried once with only the part after
// "Artist - " when nobody answers: the Soulseek server silently blocks
// searches containing some artist names (e.g. "Frank Ocean"), and the album
// title alone still finds the folders. note, if set, is told about the retry.
func searchSlskdOrAlbum(ctx context.Context, query string, progress func(int), note func(string)) ([]slskResult, error) {
	results, err := searchSlskd(ctx, query, progress)
	if err != nil || len(results) > 0 {
		return results, err
	}
	_, album, ok := strings.Cut(query, " - ")
	if !ok || slskdSearchText(album) == "" {
		return results, nil
	}
	if note != nil {
		note("Nobody answered; Soulseek may block this artist's name. Retrying with the album title only…")
	}
	log.Printf("search %q: no answers, retrying as %q", query, album)
	if results, err = searchSlskd(ctx, album, progress); err != nil {
		return results, err
	}
	// The title alone also finds other artists' folders that happen to hold a
	// song of that name ("Moon River" on a Takeshi Terauchi album), so keep
	// only results with the artist somewhere in their file paths.
	artist, _, _ := strings.Cut(query, " - ")
	kept := withArtistInPath(results, artist)
	if note != nil && len(kept) < len(results) {
		note(fmt.Sprintf("Dropped %d result(s) without %q in their folder or file names", len(results)-len(kept), artist))
	}
	return kept, nil
}

// withArtistInPath returns the results where some file path contains artist,
// compared by normTitle so punctuation and case don't matter.
func withArtistInPath(results []slskResult, artist string) []slskResult {
	want := normTitle(artist)
	if want == "" {
		return results
	}
	var kept []slskResult
	for _, r := range results {
		for _, f := range r.Files {
			if strings.Contains(normTitle(f.Filename), want) {
				kept = append(kept, r)
				break
			}
		}
	}
	return kept
}

// previewSlskd is /preview through slskd: same SSE events as the sldl path,
// plus a running count of users that have responded.
func previewSlskd(w http.ResponseWriter, r *http.Request, query string) {
	serveSSE(w, r, func(emit emitFunc) {
		start := time.Now()
		emit("[log] Searching Soulseek…")
		results, err := searchSlskdOrAlbum(r.Context(), query, func(users int) {
			if users > 0 {
				emit("[log] %d user(s) responded…", users)
			}
		}, func(msg string) { emit("[log] %s", msg) })
		switch {
		case err != nil:
			log.Printf("preview %q failed: %v", query, err)
			emit("[preview-error] Search failed: %v", err)
		default:
			log.Printf("preview %q: %d result(s) in %.1fs", query, len(results), time.Since(start).Seconds())
			if results == nil {
				results = []slskResult{}
			}
			b, _ := json.Marshal(results)
			emit("[results] %s", b)
		}
		emit("[preview-done]")
	})
}

// runSldlAndStream pushes a sldl invocation through the download queue, passes
// each output line to onLine, and returns the collected significant error
// lines plus the exit error. The process is deliberately not tied to any
// request context: a download must survive the browser tab closing.
func runSldlAndStream(onLine func(string), onStart func(*os.Process), args []string, outDir string) (sigLines []string, startTime time.Time, exitErr error) {
	job := &DownloadJob{
		cmd:     exec.Command(sldlBinary, args...),
		outDir:  outDir,
		output:  make(chan string, 200),
		exitErr: make(chan error, 1),
		onStart: onStart,
	}

	jobQueue <- job

	for line := range job.output {
		if isSignificant(line) {
			sigLines = append(sigLines, line)
		}
		onLine(line)
	}
	exitErr = <-job.exitErr
	startTime = job.startTime
	return
}

func handleDownload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req DownloadRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if req.Query == "" {
		http.Error(w, "missing query", http.StatusBadRequest)
		return
	}

	username := os.Getenv("SLDL_USER")
	password := os.Getenv("SLDL_PASS")
	if username == "" || password == "" {
		http.Error(w, "SLDL_USER or SLDL_PASS not set", http.StatusInternalServerError)
		return
	}

	outDir := musicDir
	if req.Subfolder != "" {
		outDir = filepath.Join(musicDir, filepath.FromSlash(req.Subfolder))
		if !strings.HasPrefix(outDir, musicDir) {
			http.Error(w, "invalid subfolder", http.StatusBadRequest)
			return
		}
	}

	job := newJob(req)
	go runDownload(job, req, username, password, outDir)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"id": job.ID})
}

// runDownload is the whole download pipeline for one job: wait for the
// download lock, run sldl (plus the name-search fallback), then tag with beets.
// Progress goes into the job, which the UI polls via /jobs.
func runDownload(job *Job, req DownloadRequest, username, password, outDir string) {
	bitrate := minBitrate
	if req.LowBitrate {
		bitrate = ""
	}

	ahead := downloadsPending.Add(1) - 1
	defer downloadsPending.Add(-1)
	if ahead > 0 {
		job.update(func(j *Job) { j.Phase = fmt.Sprintf("Waiting for %d download(s) ahead", ahead) })
	}
	downloadMu.Lock()
	defer downloadMu.Unlock()

	// A job cancelled while queued is already finished; don't revive it.
	if !job.enter("downloading", "Connecting to Soulseek") {
		return
	}
	onLine := func(line string) { job.sldlLine(line) }
	onStart := func(p *os.Process) {
		job.update(func(j *Job) {
			j.proc = p
			if j.cancelled {
				p.Kill()
			}
		})
	}

	var saved, sigLines []string
	var startTime time.Time
	var exitErr error
	if req.Album && len(req.Tracks) > 0 && strings.HasPrefix(req.Query, "slsk://") {
		startTime = time.Now()
		need := len(req.Tracks)
		if req.Expect > 0 && req.Expect < need {
			need = req.Expect
		}
		saved, sigLines, exitErr = downloadAlbumSource(job, req.Query, bitrate, req.Tracks, need, len(req.Alternatives) > 0, username, password, outDir, onLine, onStart)
	} else {
		args := buildSldlArgs(username, password, outDir, req.Query, bitrate, req.Album, "--no-progress")
		sigLines, startTime, exitErr = runSldlAndStream(onLine, onStart, args, outDir)
		saved = findSavedAlbums(outDir, startTime)
	}
	if job.isCancelled() {
		finishCancelled(job, outDir, startTime)
		return
	}

	// If a slsk:// source produced no usable album (most often an offline or
	// slot-starved user), look for another copy. An album is only taken from a
	// folder holding the full tracklist at the bitrate floor, so a failed source
	// is never replaced by some other release.
	if len(saved) == 0 && strings.HasPrefix(req.Query, "slsk://") {
		removeFailedParents(outDir, findFailedAlbums(outDir, startTime), job.addLog)
		var fbSaved []string
		var fbSig []string
		var fbErr error
		var cancelled bool
		if req.Album {
			fbSaved, fbSig, fbErr, cancelled = fallbackAlbum(job, req, username, password, outDir, onLine, onStart)
		} else {
			fbSaved, fbSig, fbErr, cancelled = fallbackTrack(job, req, username, password, outDir, onLine, onStart)
		}
		if cancelled {
			finishCancelled(job, outDir, startTime)
			return
		}
		sigLines = append(sigLines, fbSig...)
		saved = fbSaved
		if len(saved) > 0 {
			exitErr = nil
		} else if fbErr != nil {
			exitErr = fbErr
		}
	}

	// Logged as well as recorded: the client may have closed the tab long ago.
	if exitErr != nil && len(saved) == 0 {
		log.Printf("download %q failed: %v", req.Query, exitErr)
		job.finish("failed", func(j *Job) {
			if j.Error == "" {
				j.Error = "Download failed"
			}
			j.Details = append(j.Details, fmt.Sprintf("sldl exited: %v", exitErr))
			j.Details = append(j.Details, sigLines...)
		})
		return
	}
	if len(saved) == 0 {
		job.finish("done", func(j *Job) { j.Phase = "Nothing new was downloaded" })
		return
	}

	log.Printf("download %q saved to %s", req.Query, saved[0])
	// Tagging can't be cancelled: the files are complete by now.
	if !job.enter("tagging", "Matching against MusicBrainz") {
		finishCancelled(job, outDir, startTime)
		return
	}
	job.update(func(j *Job) { j.Saved = saved[0] })
	saveJobs()
	runBeets(func(format string, a ...any) { job.beetsLine(fmt.Sprintf(format, a...)) }, saved[0])
	job.finish("done", func(j *Job) {
		if j.Tag == nil {
			j.Tag = &JobTag{Status: "error"}
		}
	})
}

// --- slskd search ---

// slskdURL points at a search-only slskd logged in as the preview account.
// When set, previews and the download fallback search through it: it stays
// logged in, so a search takes seconds instead of a fresh sldl login plus
// sldl's long result wait. Unset, both fall back to sldl.
var slskdURL = strings.TrimRight(os.Getenv("SLSKD_URL"), "/")

const (
	// slskd ends a search this long after the last response. Nearly all
	// responses arrive in the first 2-3s; the rest are slow stragglers.
	slskdIdleTimeout = 3000 // ms
	// slskdMaxSearch cancels a search that keeps trickling in.
	slskdMaxSearch = 8 * time.Second
)

var slskdClient = &http.Client{Timeout: 15 * time.Second}

type slskdResponse struct {
	Username          string  `json:"username"`
	HasFreeUploadSlot bool    `json:"hasFreeUploadSlot"`
	UploadSpeed       float64 `json:"uploadSpeed"` // bytes/s
	Files             []struct {
		Filename string `json:"filename"`
		Length   int    `json:"length"`
		BitRate  int    `json:"bitRate"`
		Size     int64  `json:"size"`
	} `json:"files"`
}

func slskdCall(ctx context.Context, method, path string, body, out any) error {
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, slskdURL+"/api/v0"+path, rd)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := slskdClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 300))
		return fmt.Errorf("slskd %s %s: %s %s", method, path, resp.Status, strings.TrimSpace(string(msg)))
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}

// slskdSearchText turns "Artist - Album" into Soulseek search words.
// Soulseek matches whole words, so punctuation splits words ("nostalgia,ULTRA."
// matches nothing; "nostalgia ULTRA" does), and one-letter leftovers like the
// "s" of "What's" are dropped. This also removes "-", which would exclude.
func slskdSearchText(q string) string {
	parts := strings.FieldsFunc(q, func(r rune) bool { return !unicode.IsLetter(r) && !unicode.IsDigit(r) })
	var words []string
	for _, w := range parts {
		if len([]rune(w)) > 1 {
			words = append(words, w)
		}
	}
	if len(words) == 0 {
		return strings.Join(parts, " ")
	}
	return strings.Join(words, " ")
}

func newUUID() string {
	b := make([]byte, 16)
	rand.Read(b)
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

// searchSlskd runs one Soulseek search through slskd and returns the matching
// files grouped into one result per user folder (the shape sldl's
// `--print json-all` gives), uploaders with a free slot and then the fastest
// first. progress, if set, gets the number of users that have responded.
func searchSlskd(ctx context.Context, query string, progress func(users int)) ([]slskResult, error) {
	id := newUUID()
	err := slskdCall(ctx, http.MethodPost, "/searches", map[string]any{
		"id": id, "searchText": slskdSearchText(query), "searchTimeout": slskdIdleTimeout,
	}, nil)
	if err != nil {
		return nil, err
	}
	// slskd keeps every search until deleted.
	defer slskdCall(context.Background(), http.MethodDelete, "/searches/"+id, nil, nil)

	deadline := time.Now().Add(slskdMaxSearch)
	stopped, seen := false, -1
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
		var st struct {
			IsComplete    bool `json:"isComplete"`
			ResponseCount int  `json:"responseCount"`
		}
		if err := slskdCall(ctx, http.MethodGet, "/searches/"+id, nil, &st); err != nil {
			return nil, err
		}
		if progress != nil && st.ResponseCount != seen {
			seen = st.ResponseCount
			progress(seen)
		}
		if st.IsComplete {
			break
		}
		// Responses are only served once a search ends, so stop stragglers.
		if !stopped && time.Now().After(deadline) {
			stopped = true
			if err := slskdCall(ctx, http.MethodPut, "/searches/"+id, nil, nil); err != nil {
				return nil, err
			}
		}
	}

	// slskd reports a search complete a moment before it has stored the
	// responses, so re-read until they match the count (or give up after 3s).
	var responses []slskdResponse
	for try := 0; ; try++ {
		if err := slskdCall(ctx, http.MethodGet, "/searches/"+id+"/responses", nil, &responses); err != nil {
			return nil, err
		}
		var st struct {
			ResponseCount int `json:"responseCount"`
		}
		if err := slskdCall(ctx, http.MethodGet, "/searches/"+id, nil, &st); err != nil {
			return nil, err
		}
		if len(responses) >= st.ResponseCount || try == 12 {
			break
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
	}
	sort.SliceStable(responses, func(i, j int) bool {
		a, b := responses[i], responses[j]
		if a.HasFreeUploadSlot != b.HasFreeUploadSlot {
			return a.HasFreeUploadSlot
		}
		return a.UploadSpeed > b.UploadSpeed
	})
	var results []slskResult
	for _, resp := range responses {
		folders := map[string]int{} // folder -> index in results
		for _, f := range resp.Files {
			dir := f.Filename[:max(strings.LastIndex(f.Filename, `\`), 0)]
			i, ok := folders[dir]
			if !ok {
				var r slskResult
				r.User.Username = resp.Username
				r.User.HasFreeUploadSlot = resp.HasFreeUploadSlot
				r.User.UploadSpeed = resp.UploadSpeed / 1e6
				results = append(results, r)
				i = len(results) - 1
				folders[dir] = i
			}
			results[i].Files = append(results[i].Files, struct {
				Filename string
				Length   int
				Bitrate  int
				Size     int64
			}{f.Filename, f.Length, f.BitRate, f.Size})
		}
	}
	return results, nil
}

// stallTimeout is how long a source may keep every track waiting (queued
// behind other users, or not answering) before killIfStalled gives up on it.
const stallTimeout = 2 * time.Minute

// killIfStalled kills the job's sldl once stallTimeout passes without any
// track starting, so the fallback moves on to another full copy instead of
// sitting in one uploader's queue for up to sldlMaxStaleTime per track. Call
// the returned stop when sldl exits.
func killIfStalled(job *Job) (stop func()) {
	done := make(chan struct{})
	go func() {
		start := time.Now()
		tick := time.NewTicker(10 * time.Second)
		defer tick.Stop()
		for {
			select {
			case <-done:
				return
			case <-tick.C:
			}
			finished := false
			job.update(func(j *Job) {
				for _, t := range j.Tracks {
					if t.State != "waiting" {
						finished = true // the source is delivering
						return
					}
				}
				if time.Since(start) >= stallTimeout && j.proc != nil {
					finished = true
					j.proc.Kill()
					j.Notes = append(j.Notes, fmt.Sprintf("%s kept every track queued for %s, so it was dropped", j.Source, stallTimeout))
				}
			})
			if finished {
				return
			}
		}
	}()
	return func() { close(done) }
}

// fallbackAlbum downloads another copy holding the whole req.Tracks at the
// bitrate floor: req.Alternatives when the preview sent any, else the best
// folders a search for req.FallbackQuery finds, trying the next when a source
// fails. Without a tracklist nothing is tried.
func fallbackAlbum(job *Job, req DownloadRequest, username, password, outDir string, onLine func(string), onStart func(*os.Process)) (saved, sigLines []string, exitErr error, cancelled bool) {
	if req.FallbackQuery == "" || len(req.Tracks) == 0 {
		job.update(func(j *Job) {
			j.Notes = append(j.Notes, "Source failed; no tracklist to check other sources against, so none were tried")
		})
		return
	}
	failedUser := ""
	if u, err := url.Parse(req.Query); err == nil {
		failedUser = u.Host
	}
	job.update(func(j *Job) {
		j.Tracks = nil
		j.Error = ""
		j.Phase = "That source failed, looking for another full copy"
	})
	job.addLog(fmt.Sprintf("[fallback] direct URL failed; searching %q for all %d tracks at %sk+", req.FallbackQuery, len(req.Tracks), minBitrate))

	var cands []string
	for _, a := range req.Alternatives {
		if u, err := url.Parse(a); err == nil && u.Scheme == "slsk" && u.Host != failedUser {
			cands = append(cands, a)
		}
	}
	var sig []string
	if len(cands) > 0 {
		job.addLog(fmt.Sprintf("[fallback] %d other full copies from the preview", len(cands)))
	} else if cands, sig, exitErr = searchFallback(job, req, username, password, outDir, failedUser, onStart); exitErr != nil {
		return nil, sig, exitErr, false
	}
	if job.isCancelled() {
		return nil, nil, nil, true
	}
	if len(cands) == 0 {
		job.update(func(j *Job) {
			j.Error = "No other source has the full album at " + minBitrate + "k+"
			j.Notes = append(j.Notes, fmt.Sprintf("Source %s failed; no other copy of all %d tracks at %sk+ found", failedUser, len(req.Tracks), minBitrate))
		})
		return nil, sig, nil, false
	}
	return tryFallbackCandidates(job, cands, req.Tracks, username, password, outDir, onLine, onStart)
}

// searchFallback searches req.FallbackQuery and returns the full copies at
// the bitrate floor, best first.
func searchFallback(job *Job, req DownloadRequest, username, password, outDir, failedUser string, onStart func(*os.Process)) (cands, sig []string, err error) {
	var results []slskResult
	if slskdURL != "" {
		var err error
		if results, err = searchSlskdOrAlbum(context.Background(), req.FallbackQuery, nil, job.addLog); err != nil {
			return nil, nil, err
		}
	} else {
		// The search's JSON is collected, not parsed as download progress.
		var out []string
		searchArgs := buildSldlArgs(username, password, "", req.FallbackQuery, "", true, "--print", "json-all")
		sig, _, err = runSldlAndStream(func(l string) { out = append(out, l) }, onStart, searchArgs, outDir)
		if !extractJSONArray(strings.Join(out, "\n"), &results) && err != nil {
			return nil, sig, err
		}
	}
	cands = fallbackCandidates(results, req.Tracks, failedUser)
	job.addLog(fmt.Sprintf("[fallback] %d result(s), %d with the full tracklist at %sk+", len(results), len(cands), minBitrate))
	return cands, sig, nil
}

// tryFallbackCandidates downloads the first of cands (at most 3 tried) that
// saves an album.
func tryFallbackCandidates(job *Job, cands []string, tracks []ExpectedTrack, username, password, outDir string, onLine func(string), onStart func(*os.Process)) (saved, sigLines []string, exitErr error, cancelled bool) {
	const maxTries = 3
	for i, cand := range cands[:min(len(cands), maxTries)] {
		user := strings.TrimPrefix(cand, "slsk://")
		user = user[:strings.Index(user, "/")]
		job.update(func(j *Job) {
			j.Tracks = nil
			j.Error = ""
			j.Source = user
			j.Phase = "Trying another source"
			j.Notes = append(j.Notes, fmt.Sprintf("Trying %s (full tracklist, %d of %d)", user, i+1, min(len(cands), maxTries)))
		})
		job.addLog(fmt.Sprintf("[fallback] trying %s", cand))
		start := time.Now()
		saved, sig, err := downloadAlbumSource(job, cand, minBitrate, tracks, len(tracks), i < min(len(cands), maxTries)-1, username, password, outDir, onLine, onStart)
		if job.isCancelled() {
			return nil, nil, nil, true
		}
		sigLines = append(sigLines, sig...)
		if len(saved) > 0 {
			return saved, sigLines, nil, false
		}
		exitErr = err
		removeFailedParents(outDir, findFailedAlbums(outDir, start), job.addLog)
	}
	if exitErr == nil {
		exitErr = fmt.Errorf("no source delivered all %d tracks", len(tracks))
	}
	job.update(func(j *Job) {
		j.Error = fmt.Sprintf("Tried %d sources, none delivered every track", min(len(cands), maxTries))
	})
	return nil, sigLines, exitErr, false
}

// downloadAlbumSource downloads one slsk:// album folder and checks the
// result against tracks, of which it needs need. sldl keeps a partial folder in place
// (--failed-album-path disable), and when tracks are missing the same source
// is asked once more: sldl skips the files it already has, so that run only
// fetches the ones that failed (usually a dropped transfer). A folder still
// incomplete after that loses this run's files. stall drops a source that
// keeps everything queued (see killIfStalled); pass it only when another
// source is left to try.
func downloadAlbumSource(job *Job, src, bitrate string, tracks []ExpectedTrack, need int, stall bool, username, password, outDir string, onLine func(string), onStart func(*os.Process)) (saved, sigLines []string, exitErr error) {
	var start time.Time
	user := strings.TrimPrefix(src, "slsk://")
	user = user[:max(strings.Index(user, "/"), 0)]
	for attempt := 1; attempt <= 2; attempt++ {
		args := buildSldlArgs(username, password, outDir, src, bitrate, true, "--no-progress", "--failed-album-path", "disable")
		stop := func() {}
		if stall {
			stop = killIfStalled(job)
		}
		sig, st, err := runSldlAndStream(onLine, onStart, args, outDir)
		stop()
		sigLines, exitErr = append(sigLines, sig...), err
		if attempt == 1 {
			start = st
		}
		if job.isCancelled() {
			return nil, sigLines, nil
		}
		dirs := findSavedAlbums(outDir, start)
		if len(dirs) == 0 {
			return nil, sigLines, exitErr
		}
		got := localTrackMatches(dirs[0], tracks)
		if got >= need {
			return dirs, sigLines, nil
		}
		if attempt == 1 {
			job.addLog(fmt.Sprintf("[retry] got %d of %d tracks; asking %s again for the rest", got, len(tracks), user))
			job.update(func(j *Job) {
				j.Notes = append(j.Notes, fmt.Sprintf("%s delivered %d of %d tracks, retrying the rest", user, got, len(tracks)))
			})
			continue
		}
		job.update(func(j *Job) {
			j.Notes = append(j.Notes, fmt.Sprintf("%s still missing %d of %d tracks, dropped", user, len(tracks)-got, len(tracks)))
		})
		for _, d := range dirs {
			removeNewFiles(d, start, job.addLog)
		}
	}
	return nil, sigLines, exitErr
}

// localTrackMatches counts the tracks with a file in dir (no length check).
func localTrackMatches(dir string, tracks []ExpectedTrack) int {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0
	}
	var r slskResult
	for _, e := range entries {
		if !e.IsDir() {
			r.Files = append(r.Files, struct {
				Filename string
				Length   int // seconds
				Bitrate  int
				Size     int64
			}{Filename: e.Name()})
		}
	}
	return countTrackMatches(r, tracks)
}

// removeNewFiles deletes the files in dir modified since start (this
// download's), then dir itself if that left it empty, so an album folder that
// was already in the library keeps its old files.
func removeNewFiles(dir string, start time.Time, log func(string)) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if info, err := e.Info(); err == nil && !e.IsDir() && info.ModTime().After(start) {
			os.Remove(filepath.Join(dir, e.Name()))
		}
	}
	if os.Remove(dir) == nil {
		log(fmt.Sprintf("[cleanup] removed incomplete %s", dir))
	} else {
		log(fmt.Sprintf("[cleanup] removed this download's files from %s", dir))
	}
}

// fallbackTrack retries a single song as a name search for req.FallbackQuery,
// always at the bitrate floor; sldl matches the title itself.
func fallbackTrack(job *Job, req DownloadRequest, username, password, outDir string, onLine func(string), onStart func(*os.Process)) (saved, sigLines []string, exitErr error, cancelled bool) {
	if req.FallbackQuery == "" {
		return
	}
	job.update(func(j *Job) {
		j.Tracks = nil
		j.Phase = "That source failed, searching for others"
		j.Notes = append(j.Notes, fmt.Sprintf("Source %s failed; retried as a search for %q", j.Source, req.FallbackQuery))
	})
	job.addLog(fmt.Sprintf("[fallback] direct URL failed; retrying with search %q", req.FallbackQuery))
	args := buildSldlArgs(username, password, outDir, req.FallbackQuery, minBitrate, false, "--no-progress")
	sigLines, start, exitErr := runSldlAndStream(onLine, onStart, args, outDir)
	if job.isCancelled() {
		return nil, nil, nil, true
	}
	return findSavedAlbums(outDir, start), sigLines, exitErr, false
}

// --- Download jobs ---

const (
	jobsFile       = "/data/jobs.json"
	maxJobs        = 50
	maxJobLog      = 3000
	maxSavedJobLog = 300
)

type JobTrack struct {
	Name  string `json:"name"`
	State string `json:"state"` // waiting, downloading, done, failed
}

type JobTag struct {
	Status     string `json:"status"` // matched, skipped, error
	Reason     string `json:"reason,omitempty"`
	Release    string `json:"release,omitempty"`
	Similarity string `json:"similarity,omitempty"`
	URL        string `json:"url,omitempty"`
	Changes    int    `json:"changes"`
}

// Job is one download as the downloads panel sees it. All fields are guarded
// by jobsMu; sldl/beets output is parsed into Tracks/Tag as it arrives so the
// UI never has to interpret raw logs.
type Job struct {
	ID       string     `json:"id"`
	Label    string     `json:"label"`
	Query    string     `json:"query"`
	Source   string     `json:"source,omitempty"`
	Status   string     `json:"status"` // queued, downloading, tagging, done, failed, interrupted
	Phase    string     `json:"phase"`
	Tracks   []JobTrack `json:"tracks"`
	Saved    string     `json:"saved,omitempty"`
	Tag      *JobTag    `json:"tag,omitempty"`
	Error    string     `json:"error,omitempty"`
	Details  []string   `json:"details,omitempty"`
	Notes    []string   `json:"notes,omitempty"`
	Created  time.Time  `json:"created"`
	Finished *time.Time `json:"finished,omitempty"`
	Log      []string   `json:"log,omitempty"`

	beetsExpect string      // which beets value the next output line carries
	cancelled   bool        // set by /jobs/cancel
	proc        *os.Process // the running sldl, killed on cancel
}

var (
	jobsMu sync.Mutex
	jobs   []*Job // newest first
)

func newJob(req DownloadRequest) *Job {
	j := &Job{
		ID:      strconv.FormatInt(time.Now().UnixNano(), 36),
		Label:   req.Label,
		Query:   req.Query,
		Status:  "queued",
		Phase:   "Queued",
		Created: time.Now(),
	}
	if j.Label == "" {
		j.Label = req.Query
	}
	if u, err := url.Parse(req.Query); err == nil && u.Scheme == "slsk" {
		j.Source = u.Host
	}
	jobsMu.Lock()
	jobs = append([]*Job{j}, jobs...)
	trimJobsLocked()
	jobsMu.Unlock()
	saveJobs()
	return j
}

func (j *Job) active() bool {
	return j.Status == "queued" || j.Status == "downloading" || j.Status == "tagging"
}

func (j *Job) update(fn func(j *Job)) {
	jobsMu.Lock()
	fn(j)
	jobsMu.Unlock()
}

func (j *Job) setStatus(status, phase string) {
	j.update(func(j *Job) { j.Status, j.Phase = status, phase })
	saveJobs()
}

// enter moves the job to status unless it has been cancelled, in which case it
// reports false. Checked under the lock so a cancel can't be overwritten.
func (j *Job) enter(status, phase string) bool {
	ok := true
	j.update(func(j *Job) {
		if j.cancelled {
			ok = false
			return
		}
		j.Status, j.Phase = status, phase
	})
	if ok {
		saveJobs()
	}
	return ok
}

func (j *Job) isCancelled() bool {
	jobsMu.Lock()
	defer jobsMu.Unlock()
	return j.cancelled
}

// finishCancelled deletes whatever the cancelled download wrote, so no half
// album is left in the library.
func finishCancelled(job *Job, outDir string, since time.Time) {
	n := removeFilesSince(outDir, since, job.addLog)
	log.Printf("download %q cancelled; removed %d file(s)", job.Query, n)
	job.finish("cancelled", func(j *Job) {
		j.proc = nil
		j.Phase = fmt.Sprintf("Cancelled. Removed %d partial file(s)", n)
	})
}

// removeFilesSince deletes files under dir modified after since, then any
// directories that leaves empty (dir itself too, unless it is the music root).
// downloadMu guarantees nothing else downloads into the library meanwhile.
func removeFilesSince(dir string, since time.Time, logf func(string)) int {
	if since.IsZero() {
		return 0
	}
	n := 0
	var dirs []string
	filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() {
			dirs = append(dirs, path)
			return nil
		}
		if info, err := d.Info(); err == nil && info.ModTime().After(since) {
			if os.Remove(path) == nil {
				n++
				logf("[cancel] removed " + path)
			}
		}
		return nil
	})
	// Deepest first, so parents empty out after their children.
	sort.Slice(dirs, func(a, b int) bool { return len(dirs[a]) > len(dirs[b]) })
	for _, d := range dirs {
		if d != musicDir {
			os.Remove(d) // fails harmlessly unless empty
		}
	}
	return n
}

func (j *Job) finish(status string, fn func(j *Job)) {
	j.update(func(j *Job) {
		now := time.Now()
		j.Status, j.Finished = status, &now
		fn(j)
	})
	saveJobs()
}

func (j *Job) addLog(line string) {
	j.update(func(j *Job) { j.appendLogLocked(line) })
}

func (j *Job) appendLogLocked(line string) {
	j.Log = append(j.Log, line)
	if len(j.Log) > maxJobLog {
		j.Log = j.Log[len(j.Log)-maxJobLog:]
	}
}

func (j *Job) setTrackLocked(name, state string) {
	for i := range j.Tracks {
		if j.Tracks[i].Name == name {
			// A finished track can't go back to waiting on a late status line.
			if s := j.Tracks[i].State; s != "done" && s != "failed" || state == "done" {
				j.Tracks[i].State = state
			}
			return
		}
	}
	j.Tracks = append(j.Tracks, JobTrack{Name: name, State: state})
}

var (
	ansiRe = regexp.MustCompile(`\x1b\[[0-9;]*m`)
	// "Succeeded:     user\..\01 Song.mp3 [196s/160kbps/3.8MB]", see sldl's
	// DownloadWrapper; the state is a TransferStates name or "Queued (R)".
	sldlTrackRe = regexp.MustCompile(`^([A-Za-z]+(?:, [A-Za-z]+)*|Queued \([RL]\))(?: \([\d.]+MB\))?:\s+(.*\\.+ \[[^\]]*\])$`)
	sldlTotalRe = regexp.MustCompile(`^Downloading (\d+) tracks`)
	// Album mode lists the chosen folder up front: "[01]    01 Tuesday.mp3 [-1s/3.8MB]".
	sldlListedRe   = regexp.MustCompile(`^\[\d+\]\s+(.+) \[[^\]]*\]$`)
	beetsSimilarRe = regexp.MustCompile(`\(Similarity: ([\d.]+%)\)`)
	// Newer beets prints "Match (99.0%):", then the release and its URL.
	beetsMatchRe = regexp.MustCompile(`^Match \(([\d.]+%)\):$`)
)

func sldlTrackState(state string) string {
	switch {
	case strings.Contains(state, "Succeeded"):
		return "done"
	case strings.Contains(state, "InProgress"):
		return "downloading"
	case strings.Contains(state, "TimedOut"), strings.Contains(state, "Errored"),
		strings.Contains(state, "Rejected"), strings.Contains(state, "Cancelled"),
		strings.Contains(state, "Aborted"):
		return "failed"
	}
	return "waiting"
}

// sldlLine records one line of sldl output and folds it into the job state.
func (j *Job) sldlLine(raw string) {
	line := strings.TrimSpace(ansiRe.ReplaceAllString(raw, ""))
	j.update(func(j *Job) {
		j.appendLogLocked(line)
		switch {
		case strings.HasPrefix(line, "Login "):
			j.Phase = "Logging in to Soulseek"
		case strings.HasPrefix(line, "Searching:"), strings.HasPrefix(line, "Album download:"):
			j.Phase = "Searching Soulseek"
		case strings.HasPrefix(line, "Getting files in folder"):
			j.Phase = "Reading the uploader's folder"
		case strings.HasPrefix(line, "User  :"):
			if strings.Contains(line, "no upload slots") {
				j.Phase = "Uploader has no free slots, waiting in their queue"
			}
		case strings.HasPrefix(line, "Disconnected"):
			j.Phase = "Reconnecting to Soulseek"
		case sldlTotalRe.MatchString(line):
			j.Phase = "Downloading"
		case strings.HasPrefix(line, "Completed:"):
			j.Phase = strings.TrimSuffix(line, ".")
		case strings.HasPrefix(line, "Not found:"):
			j.Error = "Not found on Soulseek"
		case strings.HasPrefix(line, "All downloads failed:"):
			j.Error = "Every source failed"
		case strings.HasPrefix(line, "Moving album files to"), strings.HasPrefix(line, "Deleting album files"):
			j.Error = "Album incomplete, partial files removed"
		default:
			if m := sldlListedRe.FindStringSubmatch(line); m != nil {
				if name := m[1]; audioExt[strings.ToLower(filepath.Ext(name))] {
					j.setTrackLocked(strings.TrimSuffix(name, filepath.Ext(name)), "waiting")
				}
				return
			}
			m := sldlTrackRe.FindStringSubmatch(line)
			if m == nil {
				return
			}
			name := m[2]
			if i := strings.LastIndex(name, " ["); i > 0 {
				name = name[:i]
			}
			if i := strings.LastIndexAny(name, `\/`); i >= 0 {
				name = name[i+1:]
			}
			if !audioExt[strings.ToLower(filepath.Ext(name))] {
				return
			}
			j.setTrackLocked(strings.TrimSuffix(name, filepath.Ext(name)), sldlTrackState(m[1]))
		}
	})
}

// beetsLine records one line of runBeets output and folds the match result
// into job.Tag. Beets prints the value on the line after its label.
func (j *Job) beetsLine(raw string) {
	line := strings.TrimPrefix(ansiRe.ReplaceAllString(raw, ""), "[beets] ")
	trimmed := strings.TrimSpace(line)
	j.update(func(j *Job) {
		j.appendLogLocked("[beets] " + line)
		if j.Tag == nil {
			j.Tag = &JobTag{}
		}
		switch expect := j.beetsExpect; {
		case expect == "release" && trimmed != "":
			j.Tag.Release, j.beetsExpect = trimmed, ""
			return
		case expect == "url" && trimmed != "":
			j.Tag.URL, j.beetsExpect = trimmed, ""
			return
		case expect == "from":
			j.beetsExpect = ""
			return
		}
		switch {
		case trimmed == beetsRetryLine:
			j.Tag, j.beetsExpect = &JobTag{}, ""
		case trimmed == "Tagging:", trimmed == "To:":
			j.beetsExpect = "release"
		case trimmed == "Correcting tags from:":
			j.beetsExpect = "from"
		case trimmed == "URL:":
			j.beetsExpect = "url"
		case strings.HasPrefix(trimmed, "This album is already in the library"):
			j.Tag.Reason = "already in the beets library"
		case trimmed == "Skipping.":
			j.Tag.Status = "skipped"
			if j.Tag.Reason == "" {
				j.Tag.Reason = "no confident MusicBrainz match"
			}
		case strings.HasPrefix(trimmed, "* "):
			j.Tag.Changes++
		case strings.HasPrefix(trimmed, "Could not start beets"), strings.HasPrefix(trimmed, "finished with warning"):
			if j.Tag.Status == "" {
				j.Tag.Status = "error"
			}
		case strings.HasPrefix(trimmed, "https://musicbrainz.org/") && j.Tag.URL == "":
			j.Tag.URL = trimmed
		default:
			if m := beetsSimilarRe.FindStringSubmatch(trimmed); m != nil {
				j.Tag.Similarity, j.Tag.Status = m[1], "matched"
			} else if m := beetsMatchRe.FindStringSubmatch(trimmed); m != nil {
				j.Tag.Similarity, j.Tag.Status, j.beetsExpect = m[1], "matched", "release"
			}
		}
		if j.Tag.Status == "" {
			j.Tag.Status = "error" // overwritten once a match or skip line arrives
		}
	})
}

// trimJobsLocked drops the oldest finished jobs beyond maxJobs.
func trimJobsLocked() {
	for len(jobs) > maxJobs {
		i := len(jobs) - 1
		for i >= 0 && jobs[i].active() {
			i--
		}
		if i < 0 {
			return
		}
		jobs = append(jobs[:i], jobs[i+1:]...)
	}
}

// saveJobs persists the job list (with a shortened log) so the panel survives
// container restarts. Best effort: /data may not exist outside the container.
func saveJobs() {
	jobsMu.Lock()
	out := make([]Job, len(jobs))
	for i, j := range jobs {
		out[i] = *j
		if n := len(j.Log); n > maxSavedJobLog {
			out[i].Log = j.Log[n-maxSavedJobLog:]
		}
	}
	data, err := json.Marshal(out)
	jobsMu.Unlock()
	if err != nil {
		return
	}
	tmp := jobsFile + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err == nil {
		os.Rename(tmp, jobsFile)
	}
}

// loadJobs restores saved jobs. Anything that was still running when the
// server stopped is gone for good, so it is marked interrupted.
func loadJobs() {
	data, err := os.ReadFile(jobsFile)
	if err != nil {
		return
	}
	var saved []*Job
	if err := json.Unmarshal(data, &saved); err != nil {
		log.Printf("ignoring unreadable %s: %v", jobsFile, err)
		return
	}
	for _, j := range saved {
		if j.active() {
			j.Status = "interrupted"
			j.Error = "Interrupted by a server restart"
		}
	}
	jobsMu.Lock()
	jobs = saved
	jobsMu.Unlock()
}

// handleJobs lists all jobs, newest first, without their raw logs.
func handleJobs(w http.ResponseWriter, r *http.Request) {
	jobsMu.Lock()
	out := make([]Job, len(jobs))
	for i, j := range jobs {
		out[i] = *j
		out[i].Log = nil
	}
	jobsMu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	json.NewEncoder(w).Encode(map[string]any{"jobs": out})
}

func handleJobLog(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	jobsMu.Lock()
	var lines []string
	found := false
	for _, j := range jobs {
		if j.ID == id {
			lines, found = append([]string(nil), j.Log...), true
			break
		}
	}
	jobsMu.Unlock()
	if !found {
		http.Error(w, "no such job", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	io.WriteString(w, strings.Join(lines, "\n"))
}

// handleJobCancel stops a queued or downloading job; runDownload then removes
// the files it had written. A job that is already tagging has a complete album
// and can't be cancelled.
func handleJobCancel(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	id := r.URL.Query().Get("id")
	jobsMu.Lock()
	var job *Job
	for _, j := range jobs {
		if j.ID == id {
			job = j
		}
	}
	if job == nil || (job.Status != "queued" && job.Status != "downloading") {
		jobsMu.Unlock()
		http.Error(w, "job is not queued or downloading", http.StatusConflict)
		return
	}
	job.cancelled = true
	if job.Status == "queued" {
		// Still waiting for the download lock: nothing written yet.
		now := time.Now()
		job.Status, job.Phase, job.Finished = "cancelled", "Cancelled before it started", &now
	} else {
		job.Phase = "Cancelling"
		if job.proc != nil {
			job.proc.Kill()
		}
	}
	jobsMu.Unlock()
	saveJobs()
	w.WriteHeader(http.StatusNoContent)
}

// handleJobDismiss removes one finished job, or every finished job when
// id=finished. Running and queued jobs can't be dismissed.
func handleJobDismiss(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	id := r.URL.Query().Get("id")
	jobsMu.Lock()
	kept := jobs[:0]
	for _, j := range jobs {
		if !j.active() && (id == "finished" || j.ID == id) {
			continue
		}
		kept = append(kept, j)
	}
	jobs = kept
	jobsMu.Unlock()
	saveJobs()
	w.WriteHeader(http.StatusNoContent)
}
