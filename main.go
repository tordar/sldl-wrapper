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

var jobQueue = make(chan *DownloadJob, 50)

func init() {
	go jobWorker()
}

func jobWorker() {
	for job := range jobQueue {
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

func sanitizeQuery(q string) string {
	q = strings.ReplaceAll(q, " & ", " ")
	q = strings.ReplaceAll(q, "&", " ")
	return strings.TrimSpace(q)
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

	username := os.Getenv("SLDL_USER")
	password := os.Getenv("SLDL_PASS")
	if username == "" || password == "" {
		http.Error(w, "SLDL_USER or SLDL_PASS not set", http.StatusInternalServerError)
		return
	}

	args := buildSldlArgs(username, password, "", req.Query, "", req.Album, "--print", "json-all")

	// A preview is only useful to the client that asked, so it dies with the request.
	job := &DownloadJob{
		cmd:     exec.CommandContext(r.Context(), sldlBinary, args...),
		outDir:  musicDir,
		output:  make(chan string, 200),
		exitErr: make(chan error, 1),
	}

	serveSSE(w, r, func(emit emitFunc) {
		queueLen := len(jobQueue)
		if queueLen > 0 {
			emit("[queued] %d job(s) ahead — waiting...", queueLen)
		}

		jobQueue <- job

		var allLines []string
		for line := range job.output {
			allLines = append(allLines, line)
			emit("[log] %s", line)
		}
		<-job.exitErr

		fullOutput := strings.Join(allLines, "\n")
		jsonFound := false
		for i, ch := range fullOutput {
			if ch == '[' || ch == '{' {
				var raw json.RawMessage
				if err := json.Unmarshal([]byte(fullOutput[i:]), &raw); err == nil {
					compact, _ := json.Marshal(raw)
					emit("[results] %s", compact)
					jsonFound = true
					break
				}
			}
		}
		if !jsonFound {
			rawJSON, _ := json.Marshal(fullOutput)
			emit("[results-raw] %s", rawJSON)
		}
		emit("[preview-done]")
	})
}

// runSldlAndStream pushes a sldl invocation through the worker queue, streams
// its stdout/stderr through emit, and returns the collected significant error
// lines plus the exit error. The process is deliberately not tied to the
// request context: a download must survive the browser tab closing.
func runSldlAndStream(emit emitFunc, args []string, outDir string) (sigLines []string, startTime time.Time, exitErr error) {
	job := &DownloadJob{
		cmd:     exec.Command(sldlBinary, args...),
		outDir:  outDir,
		output:  make(chan string, 200),
		exitErr: make(chan error, 1),
	}

	queueLen := len(jobQueue)
	if queueLen > 0 {
		emit("[queued] %d download(s) ahead — waiting...", queueLen)
	}

	jobQueue <- job

	for line := range job.output {
		if isSignificant(line) {
			sigLines = append(sigLines, line)
		}
		emit("%s", line)
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

	bitrate := minBitrate
	if req.LowBitrate {
		bitrate = ""
	}

	serveSSE(w, r, func(emit emitFunc) {
		ahead := downloadsPending.Add(1) - 1
		defer downloadsPending.Add(-1)
		if ahead > 0 {
			emit("[queued] %d download(s) ahead — waiting...", ahead)
		}
		downloadMu.Lock()
		defer downloadMu.Unlock()

		args := buildSldlArgs(username, password, outDir, req.Query, bitrate, req.Album)
		sigLines, startTime, exitErr := runSldlAndStream(emit, args, outDir)

		saved := findSavedAlbums(outDir, startTime)

		// If the direct sldl run produced no usable album (most often because a
		// slsk:// URL pinned us to one offline / slot-starved user), fall back to
		// a name-based search so sldl can pick any seeder.
		if len(saved) == 0 {
			if fallbackQuery, ok := parseSlskURL(req.Query); ok {
				failed := findFailedAlbums(outDir, startTime)
				removeFailedParents(outDir, failed, func(msg string) { emit("%s", msg) })

				emit("[fallback] direct URL failed; retrying with search %q", fallbackQuery)

				retryArgs := buildSldlArgs(username, password, outDir, fallbackQuery, bitrate, req.Album)
				retrySig, retryStart, retryErr := runSldlAndStream(emit, retryArgs, outDir)
				sigLines = append(sigLines, retrySig...)
				saved = findSavedAlbums(outDir, retryStart)
				if len(saved) == 0 && retryErr != nil {
					exitErr = retryErr
				} else if len(saved) > 0 {
					exitErr = nil
				}
			}
		}

		// Logged as well as emitted: the client may have closed the tab long ago.
		if exitErr != nil && len(saved) == 0 {
			log.Printf("download %q failed: %v", req.Query, exitErr)
			emit("[error] sldl exited: %v", exitErr)
			for _, l := range sigLines {
				emit("[error-detail] %s", l)
			}
			return
		}
		if len(saved) > 0 {
			log.Printf("download %q saved to %s", req.Query, saved[0])
			emit("[saved] %s", saved[0])
			runBeets(emit, saved[0])
		}
		emit("[done]")
	})
}
