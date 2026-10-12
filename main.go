package main

import (
	"bufio"
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
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
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

// parseSlskURL extracts a search query "Artist Album" from a slsk://USER/PATH URL.
// Returns ("", false) if the URL isn't usable as a fallback.
func parseSlskURL(q string) (string, bool) {
	if !strings.HasPrefix(q, "slsk://") {
		return "", false
	}
	u, err := url.Parse(q)
	if err != nil {
		return "", false
	}
	var parts []string
	for _, p := range strings.Split(u.Path, "/") {
		if p == "" {
			continue
		}
		low := strings.ToLower(p)
		// drop generic library-root names so the search isn't poisoned by them
		if low == "beets" || low == "music" || low == "shared" || low == "downloads" || low == "complete" {
			continue
		}
		parts = append(parts, p)
	}
	if len(parts) == 0 {
		return "", false
	}
	if len(parts) >= 2 {
		return parts[len(parts)-2] + " " + parts[len(parts)-1], true
	}
	return parts[0], true
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

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	w.Header().Set("Content-Type", "application/json")
	io.Copy(w, resp.Body)
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

	resp, err := http.DefaultClient.Do(req)
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
	resp, err := http.DefaultClient.Do(req)
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
	resp2, err := http.DefaultClient.Do(req2)
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

func runBeets(emit emitFunc, dir string) {
	emit("[beets] Auto-tagging with beets...")

	// -q: never prompt; apply confident MusicBrainz matches, otherwise fall
	// back to import.quiet_fallback. (-A would be --noautotag.)
	cmd := exec.Command("beet", "--config=/etc/beets/config.yaml", "import", "-q", dir)
	pr, pw, err := os.Pipe()
	if err != nil {
		emit("[beets] Could not start beets: %v", err)
		return
	}
	cmd.Stdout = pw
	cmd.Stderr = pw
	if err := cmd.Start(); err != nil {
		pw.Close()
		pr.Close()
		emit("[beets] Could not start beets: %v", err)
		return
	}
	pw.Close()

	scanner := bufio.NewScanner(pr)
	for scanner.Scan() {
		emit("[beets] %s", scanner.Text())
	}
	if err := cmd.Wait(); err != nil {
		emit("[beets] finished with warning: %v", err)
	} else {
		emit("[beets] tagging complete")
	}
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
		jsonFound := false
		for i, ch := range fullOutput {
			if ch == '[' {
				if err := json.Unmarshal([]byte(fullOutput[i:]), &results); err == nil {
					jsonFound = true
					break
				}
			}
		}

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

// runSldlAndStream pushes a sldl invocation through the download queue, passes
// each output line to onLine, and returns the collected significant error
// lines plus the exit error. The process is deliberately not tied to any
// request context: a download must survive the browser tab closing.
func runSldlAndStream(onLine func(string), args []string, outDir string) (sigLines []string, startTime time.Time, exitErr error) {
	job := &DownloadJob{
		cmd:     exec.Command(sldlBinary, args...),
		outDir:  outDir,
		output:  make(chan string, 200),
		exitErr: make(chan error, 1),
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

	job.setStatus("downloading", "Connecting to Soulseek")
	onLine := func(line string) { job.sldlLine(line) }

	args := buildSldlArgs(username, password, outDir, req.Query, bitrate, req.Album, "--no-progress")
	sigLines, startTime, exitErr := runSldlAndStream(onLine, args, outDir)

	saved := findSavedAlbums(outDir, startTime)

	// If the direct sldl run produced no usable album (most often because a
	// slsk:// URL pinned us to one offline / slot-starved user), fall back to
	// a name-based search so sldl can pick any seeder.
	if len(saved) == 0 {
		if fallbackQuery, ok := parseSlskURL(req.Query); ok {
			failed := findFailedAlbums(outDir, startTime)
			removeFailedParents(outDir, failed, job.addLog)

			job.update(func(j *Job) {
				j.Tracks = nil
				j.Phase = "That source failed, searching for others"
				j.Notes = append(j.Notes, fmt.Sprintf("Source %s failed; retried as a search for %q", j.Source, fallbackQuery))
			})
			job.addLog(fmt.Sprintf("[fallback] direct URL failed; retrying with search %q", fallbackQuery))

			retryArgs := buildSldlArgs(username, password, outDir, fallbackQuery, bitrate, req.Album, "--no-progress")
			retrySig, retryStart, retryErr := runSldlAndStream(onLine, retryArgs, outDir)
			sigLines = append(sigLines, retrySig...)
			saved = findSavedAlbums(outDir, retryStart)
			if len(saved) == 0 && retryErr != nil {
				exitErr = retryErr
			} else if len(saved) > 0 {
				exitErr = nil
			}
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
	job.update(func(j *Job) {
		j.Saved = saved[0]
		j.Status = "tagging"
		j.Phase = "Matching against MusicBrainz"
	})
	saveJobs()
	runBeets(func(format string, a ...any) { job.beetsLine(fmt.Sprintf(format, a...)) }, saved[0])
	job.finish("done", func(j *Job) {
		if j.Tag == nil {
			j.Tag = &JobTag{Status: "error"}
		}
	})
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

	beetsExpect string // which beets value the next output line carries
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
		default:
			if m := beetsSimilarRe.FindStringSubmatch(trimmed); m != nil {
				j.Tag.Similarity, j.Tag.Status = m[1], "matched"
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
