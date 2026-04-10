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
	"time"
)

const (
	sldlBinary = "/sldl/sldl"
	musicDir   = "/music"
	mbBase     = "https://musicbrainz.org/ws/2"
	userAgent  = "sldl-wrapper/1.0 (https://github.com/example/sldl-wrapper)"
)

var errorKeywords = []string{"fail", "error", "no result", "not found", "unable", "warning", "timeout", "connect"}

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

	mbURL := fmt.Sprintf("%s%s?query=%s&fmt=json&limit=20", mbBase, endpoint, url.QueryEscape(q))
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

	// Fetch the release group to get its list of releases
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

	// Pick canonical release: earliest Official, then earliest any
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

	// Fetch recordings for the chosen release
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
	fmt.Fprintf(w, `{"queued":%d}`, len(jobQueue))
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

func runPromoStrip(w http.ResponseWriter, flusher http.Flusher, dir string) {
	fmt.Fprintf(w, "data: [beets] Stripping promo tags...\n\n")
	flusher.Flush()

	cmd := exec.Command("python3", "-c", promoStripScript, dir)
	pr, pw, err := os.Pipe()
	if err != nil {
		fmt.Fprintf(w, "data: [beets] promo strip error: %v\n\n", err)
		return
	}
	cmd.Stdout = pw
	cmd.Stderr = pw
	if err := cmd.Start(); err != nil {
		pw.Close()
		pr.Close()
		fmt.Fprintf(w, "data: [beets] promo strip error: %v\n\n", err)
		return
	}
	pw.Close()
	scanner := bufio.NewScanner(pr)
	for scanner.Scan() {
		fmt.Fprintf(w, "data: [beets] %s\n\n", scanner.Text())
		flusher.Flush()
	}
	cmd.Wait()
}

func runBeets(w http.ResponseWriter, flusher http.Flusher, dir string) {
	fmt.Fprintf(w, "data: [beets] Auto-tagging with beets...\n\n")
	flusher.Flush()

	cmd := exec.Command("beet", "--config=/etc/beets/config.yaml", "import", "-A", dir)
	pr, pw, err := os.Pipe()
	if err != nil {
		fmt.Fprintf(w, "data: [beets] Could not start beets: %v\n\n", err)
		return
	}
	cmd.Stdout = pw
	cmd.Stderr = pw
	if err := cmd.Start(); err != nil {
		pw.Close()
		pr.Close()
		fmt.Fprintf(w, "data: [beets] Could not start beets: %v\n\n", err)
		return
	}
	pw.Close()

	scanner := bufio.NewScanner(pr)
	for scanner.Scan() {
		fmt.Fprintf(w, "data: [beets] %s\n\n", scanner.Text())
		flusher.Flush()
	}
	if err := cmd.Wait(); err != nil {
		fmt.Fprintf(w, "data: [beets] finished with warning: %v\n\n", err)
	} else {
		fmt.Fprintf(w, "data: [beets] tagging complete\n\n")
	}
	flusher.Flush()
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

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	runPromoStrip(w, flusher, clean)
	runBeets(w, flusher, clean)
	fmt.Fprintf(w, "data: [done]\n\n")
	flusher.Flush()
}

type DownloadRequest struct {
	Query     string `json:"query"`
	Album     bool   `json:"album"`
	Subfolder string `json:"subfolder"`
}

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

	args := []string{"--username", username, "--password", password, "--print", "json-all", "--min-bitrate", "320"}
	if req.Album {
		args = append(args, "--album")
	}
	args = append(args, req.Query)

	job := &DownloadJob{
		cmd:     exec.CommandContext(r.Context(), sldlBinary, args...),
		outDir:  musicDir,
		output:  make(chan string, 200),
		exitErr: make(chan error, 1),
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	queueLen := len(jobQueue)
	if queueLen > 0 {
		fmt.Fprintf(w, "data: [queued] %d job(s) ahead — waiting...\n\n", queueLen)
		flusher.Flush()
	}

	jobQueue <- job

	// Stream all lines as log events AND collect for JSON extraction
	var allLines []string
	for line := range job.output {
		allLines = append(allLines, line)
		fmt.Fprintf(w, "data: [log] %s\n\n", line)
		flusher.Flush()
	}
	<-job.exitErr

	// Find the JSON block: try parsing from each '[' or '{' in full output
	fullOutput := strings.Join(allLines, "\n")
	jsonFound := false
	for i, ch := range fullOutput {
		if ch == '[' || ch == '{' {
			var raw json.RawMessage
			if err := json.Unmarshal([]byte(fullOutput[i:]), &raw); err == nil {
				compact, _ := json.Marshal(raw)
				fmt.Fprintf(w, "data: [results] %s\n\n", string(compact))
				jsonFound = true
				break
			}
		}
	}
	if !jsonFound {
		rawJSON, _ := json.Marshal(fullOutput)
		fmt.Fprintf(w, "data: [results-raw] %s\n\n", string(rawJSON))
	}
	fmt.Fprintf(w, "data: [preview-done]\n\n")
	flusher.Flush()
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

	args := []string{"--username", username, "--password", password, "--path", outDir, "--min-bitrate", "320"}
	if req.Album {
		args = append(args, "--album")
	}
	args = append(args, req.Query)

	job := &DownloadJob{
		cmd:     exec.CommandContext(r.Context(), sldlBinary, args...),
		outDir:  outDir,
		output:  make(chan string, 200),
		exitErr: make(chan error, 1),
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	queueLen := len(jobQueue)
	if queueLen > 0 {
		fmt.Fprintf(w, "data: [queued] %d download(s) ahead — waiting...\n\n", queueLen)
		flusher.Flush()
	}

	jobQueue <- job

	var sigLines []string
	for line := range job.output {
		if isSignificant(line) {
			sigLines = append(sigLines, line)
		}
		fmt.Fprintf(w, "data: %s\n\n", line)
		flusher.Flush()
	}

	exitErr := <-job.exitErr

	var saved []string
	filepath.WalkDir(job.outDir, func(path string, d os.DirEntry, err error) error {
		if err != nil || !d.IsDir() || path == job.outDir {
			return nil
		}
		info, err := d.Info()
		if err == nil && info.ModTime().After(job.startTime) {
			saved = append(saved, path)
		}
		return nil
	})

	if exitErr != nil && len(saved) == 0 {
		fmt.Fprintf(w, "data: [error] sldl exited: %v\n\n", exitErr)
		for _, l := range sigLines {
			fmt.Fprintf(w, "data: [error-detail] %s\n\n", l)
		}
	} else {
		if len(saved) > 0 {
			fmt.Fprintf(w, "data: [saved] %s\n\n", saved[0])
			flusher.Flush()
			runBeets(w, flusher, saved[0])
		}
		fmt.Fprintf(w, "data: [done]\n\n")
	}
	flusher.Flush()
}
