package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	_ "embed"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"regexp"
	"runtime"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode/utf8"
)

//go:embed index.html
var indexHTML string

type Config struct {
	Addr             string
	DataDir          string
	Cookies          string
	BiliCookies      string
	ChannelsFile     string
	YTDLP            string
	Aria2            string
	Biliup           string
	DeepSeekKey      string
	DeepSeekModel    string
	DeepSeekURL      string
	DefaultTags      string
	AdminUser        string
	AdminPass        string
	SecretKey        string
	UploadTimeout    time.Duration
	QueueWaitTimeout time.Duration
	MagnetTimeout    time.Duration
	BTListenPort     string
}

type MonitoredChannel struct {
	ID                   string          `json:"id"`
	URL                  string          `json:"url"`
	Title                string          `json:"title"`
	Uploader             string          `json:"uploader"`
	Enabled              bool            `json:"enabled"`
	CheckIntervalMinutes int             `json:"check_interval_minutes"`
	Translate            bool            `json:"translate"`
	Tid                  string          `json:"tid"`
	Tags                 string          `json:"tags"`
	Quality              string          `json:"quality"`
	SplitChapters        bool            `json:"split_chapters"`
	MaxPerCheck          int             `json:"max_per_check"`
	LastCheckedAt        time.Time       `json:"last_checked_at"`
	LastSyncedAt         time.Time       `json:"last_synced_at"`
	LastSyncedTitle      string          `json:"last_synced_title"`
	LastSyncedVideoID    string          `json:"last_synced_video_id"`
	SyncCount            int             `json:"sync_count"`
	SyncedIDs            map[string]bool `json:"synced_ids"`
	CreatedAt            time.Time       `json:"created_at"`
}

type Job struct {
	ID         string       `json:"id"`
	Kind       string       `json:"kind"`   // "youtube", "magnet", "biliup", "pipeline"
	Status     string       `json:"status"` // "queued", "running", "done", "failed", "canceled"
	Step       string       `json:"step,omitempty"`
	Error      string       `json:"error,omitempty"`
	Created    time.Time    `json:"created"`
	Started    time.Time    `json:"started,omitempty"`
	Finished   time.Time    `json:"finished,omitempty"`
	Input      any          `json:"input,omitempty"`
	Output     any          `json:"output,omitempty"`
	Logs       string       `json:"logs,omitempty"`
	Progress   *JobProgress `json:"progress,omitempty"`
	ctx        context.Context
	cancelFunc context.CancelFunc
	retry      func(*Job)
}

// JobProgress is the small, tool-agnostic snapshot rendered by the queue UI.
// Downloaders, ffmpeg and uploaders can therefore expose the same feedback.
type JobProgress struct {
	Percent    float64   `json:"percent,omitempty"`
	Downloaded int64     `json:"downloaded_bytes,omitempty"`
	Total      int64     `json:"total_bytes,omitempty"`
	Speed      int64     `json:"speed_bytes,omitempty"`
	ETASeconds int64     `json:"eta_seconds,omitempty"`
	Current    string    `json:"current,omitempty"`
	Detail     string    `json:"detail,omitempty"`
	UpdatedAt  time.Time `json:"updated_at"`
}

type App struct {
	cfg           Config
	mu            sync.RWMutex
	jobs          map[string]*Job
	order         []string
	downloadSlots chan struct{}
	uploadSlots   chan struct{}
	cmu           sync.RWMutex
	channels      map[string]*MonitoredChannel
	channelOrder  []string
	smu           sync.RWMutex
	stats         AppStats
	netStats      NetworkStats
}

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func loadEnvFile(path string) {
	b, err := os.ReadFile(path)
	if err != nil {
		return
	}
	scanner := bufio.NewScanner(bytes.NewReader(b))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) == 2 {
			k := strings.TrimSpace(parts[0])
			v := strings.Trim(strings.TrimSpace(parts[1]), "\"'")
			if os.Getenv(k) == "" {
				_ = os.Setenv(k, v)
			}
		}
	}
}

func loadConfig() Config {
	loadEnvFile("/etc/y2b.env")

	key := os.Getenv("DEEPSEEK_API_KEY")
	if key == "" {
		key = os.Getenv("Y2B_LLM_API_KEY")
	}
	model := os.Getenv("DEEPSEEK_MODEL")
	if model == "" {
		model = env("Y2B_LLM_MODEL", "deepseek-chat")
	}
	apiURL := os.Getenv("DEEPSEEK_API_URL")
	if apiURL == "" {
		apiURL = env("Y2B_LLM_API_URL", "https://api.deepseek.com/v1/chat/completions")
	}

	adminUser := env("WEB_USER", "admin")
	adminPass := os.Getenv("WEB_PASSWORD")
	if adminPass == "" {
		adminPass = env("Y2B_ADMIN_PASSWORD", "y2b@vibe2026")
	}
	secretKey := env("WEB_SECRET_KEY", "y2b_jwt_secret_token_key_2026_x86")
	uploadTimeout := 4 * time.Hour
	if raw := os.Getenv("Y2B_UPLOAD_TIMEOUT"); raw != "" {
		if parsed, err := time.ParseDuration(raw); err == nil && parsed > 0 {
			uploadTimeout = parsed
		}
	}
	queueWaitTimeout := 2 * time.Hour
	if raw := os.Getenv("Y2B_QUEUE_WAIT_TIMEOUT"); raw != "" {
		if parsed, err := time.ParseDuration(raw); err == nil && parsed > 0 {
			queueWaitTimeout = parsed
		}
	}
	magnetTimeout := 30 * time.Minute
	if raw := os.Getenv("Y2B_MAGNET_TIMEOUT"); raw != "" {
		if parsed, err := time.ParseDuration(raw); err == nil && parsed > 0 {
			magnetTimeout = parsed
		}
	}
	btListenPort := env("Y2B_BT_LISTEN_PORT", "51413")

	return Config{
		Addr:             env("Y2B_ADDR", "127.0.0.1:8765"),
		DataDir:          env("Y2B_DATA", "/srv/y2b/data"),
		Cookies:          env("Y2B_COOKIES", "/srv/y2b/cookies.json"),
		BiliCookies:      env("Y2B_BILI_COOKIES", "/srv/y2b/cookies.json"),
		ChannelsFile:     env("Y2B_CHANNELS", "/srv/y2b/channels.json"),
		YTDLP:            env("Y2B_YTDLP", "/home/ubuntu/.local/bin/yt-dlp"),
		Aria2:            env("Y2B_ARIA2", "/usr/bin/aria2c"),
		Biliup:           env("Y2B_BILIUP", "/usr/local/bin/biliup"),
		DeepSeekKey:      key,
		DeepSeekModel:    model,
		DeepSeekURL:      apiURL,
		DefaultTags:      env("Y2B_TAGS", "AI,Vibe Coding,编程,教程"),
		AdminUser:        adminUser,
		AdminPass:        adminPass,
		SecretKey:        secretKey,
		UploadTimeout:    uploadTimeout,
		QueueWaitTimeout: queueWaitTimeout,
		MagnetTimeout:    magnetTimeout,
		BTListenPort:     btListenPort,
	}
}

func id() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func (a *App) jobsFilePath() string {
	return filepath.Join(a.cfg.DataDir, "jobs.json")
}

// writeAtomic keeps the last known-good state available if the process or host
// loses power while persisting a queue/configuration file.
func writeAtomic(path string, data []byte, perm os.FileMode) error {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, perm); err != nil {
		return err
	}
	if old, err := os.ReadFile(path); err == nil && len(old) > 0 {
		_ = os.WriteFile(path+".bak", old, perm)
	}
	return os.Rename(tmp, path)
}

func (a *App) saveJobs() {
	a.mu.RLock()
	defer a.mu.RUnlock()

	type persistedJob struct {
		ID       string       `json:"id"`
		Kind     string       `json:"kind"`
		Status   string       `json:"status"`
		Step     string       `json:"step,omitempty"`
		Error    string       `json:"error,omitempty"`
		Created  time.Time    `json:"created"`
		Started  time.Time    `json:"started,omitempty"`
		Finished time.Time    `json:"finished,omitempty"`
		Input    any          `json:"input,omitempty"`
		Output   any          `json:"output,omitempty"`
		Logs     string       `json:"logs,omitempty"`
		Progress *JobProgress `json:"progress,omitempty"`
	}

	list := make([]persistedJob, 0, len(a.order))
	for _, oid := range a.order {
		if j := a.jobs[oid]; j != nil {
			list = append(list, persistedJob{
				ID:       j.ID,
				Kind:     j.Kind,
				Status:   j.Status,
				Step:     j.Step,
				Error:    j.Error,
				Created:  j.Created,
				Started:  j.Started,
				Finished: j.Finished,
				Input:    j.Input,
				Output:   j.Output,
				Logs:     j.Logs,
				Progress: j.Progress,
			})
		}
	}

	b, err := json.MarshalIndent(list, "", "  ")
	if err == nil {
		_ = writeAtomic(a.jobsFilePath(), b, 0640)
	}
}

func (a *App) loadJobs() {
	b, err := os.ReadFile(a.jobsFilePath())
	var list []*Job
	if err != nil || json.Unmarshal(b, &list) != nil {
		if backup, backupErr := os.ReadFile(a.jobsFilePath() + ".bak"); backupErr == nil {
			_ = json.Unmarshal(backup, &list)
		}
	}
	if len(list) == 0 {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, j := range list {
		if j == nil || j.ID == "" {
			continue
		}
		if j.Status == "running" || j.Status == "queued" {
			j.Status = "canceled"
			j.Error = "服务重启中断"
			j.Finished = time.Now()
		}
		a.jobs[j.ID] = j
		a.order = append(a.order, j.ID)
	}
}

func (a *App) add(kind string, input any) *Job {
	ctx, cancel := context.WithCancel(context.Background())
	j := &Job{
		ID:         id(),
		Kind:       kind,
		Status:     "queued",
		Step:       "排队中",
		Created:    time.Now(),
		Input:      input,
		ctx:        ctx,
		cancelFunc: cancel,
	}
	a.mu.Lock()
	a.jobs[j.ID] = j
	a.order = append(a.order, j.ID)
	a.mu.Unlock()
	a.saveJobs()
	return j
}

func (a *App) setStep(j *Job, step string) {
	a.mu.Lock()
	j.Step = step
	if j.Progress == nil && j.Status == "running" {
		j.Progress = &JobProgress{Detail: step, UpdatedAt: time.Now()}
	} else if j.Progress != nil && j.Progress.Percent == 0 && j.Progress.Downloaded == 0 {
		j.Progress.Detail = step
	}
	a.mu.Unlock()
	a.saveJobs()
}

func (a *App) setProgress(j *Job, p JobProgress) {
	if p.Percent < 0 {
		p.Percent = 0
	}
	if p.Percent > 100 {
		p.Percent = 100
	}
	p.UpdatedAt = time.Now()
	a.mu.Lock()
	j.Progress = &p
	a.mu.Unlock()
}

func (a *App) set(j *Job, status, err string, out any, logs string) {
	a.mu.Lock()
	j.Status = status
	j.Error = err
	j.Output = out
	if logs != "" {
		if len(logs) > 64*1024 {
			logs = logs[len(logs)-64*1024:] // Keep latest 64KB to avoid RAM growth
		}
		j.Logs = logs
	}
	if status == "running" {
		j.Started = time.Now()
		if j.Step == "" || j.Step == "排队中" {
			j.Step = "执行中"
		}
		if j.Progress == nil {
			j.Progress = &JobProgress{Detail: j.Step, UpdatedAt: time.Now()}
		}
	} else if status == "done" {
		j.Finished = time.Now()
		j.Step = "已完成"
	} else if status == "failed" {
		j.Finished = time.Now()
		j.Step = "失败"
	} else if status == "canceled" {
		j.Finished = time.Now()
		j.Step = "已取消"
	}
	a.mu.Unlock()
	a.saveJobs()
}

func (a *App) ensureSafeMemory(ctx context.Context) error {
	for i := 0; i < 15; i++ {
		mem := getMemoryInfo()
		if mem.AvailableMB >= 80 || mem.TotalMB == 0 {
			return nil
		}
		// Memory is tight! Trigger aggressive GC to free memory
		runtime.GC()
		debug.FreeOSMemory()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(1 * time.Second):
		}
	}
	return nil
}

func startMemoryWatchdog() {
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			runtime.GC()
			debug.FreeOSMemory()
		}
	}()
}

func (a *App) runWithSlot(j *Job, slot chan struct{}, fn func() (any, string, error)) {
	if err := a.acquireSlot(j.ctx, slot); err != nil {
		if errors.Is(err, context.Canceled) {
			a.set(j, "canceled", "", nil, "")
		} else {
			a.set(j, "failed", err.Error(), nil, "")
		}
		return
	}
	defer func() {
		<-slot
		runtime.GC()
		debug.FreeOSMemory()
	}()

	a.mu.RLock()
	canceled := j.Status == "canceled"
	a.mu.RUnlock()
	if canceled {
		return
	}

	// Pre-flight memory safety check
	if err := a.ensureSafeMemory(j.ctx); err != nil {
		a.set(j, "canceled", "等待空闲内存超时或被取消", nil, "")
		return
	}

	a.set(j, "running", "", nil, "")
	out, logs, err := fn()

	a.mu.RLock()
	canceled = j.Status == "canceled"
	a.mu.RUnlock()
	if canceled {
		return
	}

	if err != nil {
		a.set(j, "failed", err.Error(), out, logs)
	} else {
		a.set(j, "done", "", out, logs)
	}
}

func (a *App) acquireSlot(ctx context.Context, slot chan struct{}) error {
	if a.cfg.QueueWaitTimeout <= 0 {
		select {
		case slot <- struct{}{}:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	timer := time.NewTimer(a.cfg.QueueWaitTimeout)
	defer timer.Stop()
	select {
	case slot <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return fmt.Errorf("队列等待超时（超过 %s）", a.cfg.QueueWaitTimeout)
	}
}

func (a *App) listJobs(status, kind string) []*Job {
	a.mu.RLock()
	defer a.mu.RUnlock()
	result := make([]*Job, 0, len(a.order))
	for i := len(a.order) - 1; i >= 0; i-- {
		j := a.jobs[a.order[i]]
		if j == nil {
			continue
		}
		if status != "" && j.Status != status {
			continue
		}
		if kind != "" && j.Kind != kind {
			continue
		}
		// The queue view does not need live logs. Omitting them keeps a single
		// polling response small even after many ffmpeg/aria2c jobs.
		copy := *j
		copy.Logs = ""
		result = append(result, &copy)
	}
	return result
}

func (a *App) cancelJob(id string) error {
	a.mu.Lock()
	j := a.jobs[id]
	if j == nil {
		a.mu.Unlock()
		return errors.New("job not found")
	}
	if j.Status != "queued" && j.Status != "running" {
		a.mu.Unlock()
		return fmt.Errorf("only queued or running jobs can be canceled (status=%s)", j.Status)
	}
	j.Status = "canceled"
	j.Step = "已取消"
	j.Finished = time.Now()
	if j.cancelFunc != nil {
		j.cancelFunc()
	}
	a.mu.Unlock()
	a.saveJobs()
	return nil
}

func (a *App) deleteJob(id string) error {
	a.mu.Lock()
	j := a.jobs[id]
	if j == nil {
		a.mu.Unlock()
		return errors.New("job not found")
	}
	if j.Status == "queued" || j.Status == "running" {
		a.mu.Unlock()
		return errors.New("queued or running jobs cannot be deleted")
	}
	delete(a.jobs, id)
	newOrder := make([]string, 0, len(a.order))
	for _, oid := range a.order {
		if oid != id {
			newOrder = append(newOrder, oid)
		}
	}
	a.order = newOrder
	a.mu.Unlock()
	a.saveJobs()
	return nil
}

func (a *App) clearFinishedJobs() int {
	a.mu.Lock()
	count := 0
	newOrder := make([]string, 0, len(a.order))
	for _, oid := range a.order {
		j := a.jobs[oid]
		if j != nil && (j.Status == "done" || j.Status == "failed" || j.Status == "canceled") {
			delete(a.jobs, oid)
			count++
		} else if j != nil {
			newOrder = append(newOrder, oid)
		}
	}
	a.order = newOrder
	a.mu.Unlock()
	a.saveJobs()
	return count
}

func (a *App) dispatchJob(j *Job) {
	if j == nil {
		return
	}
	switch j.Kind {
	case "youtube":
		var q youtubeReq
		if b, err := json.Marshal(j.Input); err == nil {
			_ = json.Unmarshal(b, &q)
			j.retry = a.createYoutubeHandler(q)
		}
	case "magnet":
		var q magnetReq
		if b, err := json.Marshal(j.Input); err == nil {
			_ = json.Unmarshal(b, &q)
			j.retry = a.createMagnetHandler(q)
		}
	case "biliup":
		var q uploadReq
		if b, err := json.Marshal(j.Input); err == nil {
			_ = json.Unmarshal(b, &q)
			j.retry = a.createUploadHandler(q)
		}
	case "pipeline":
		var q pipelineReq
		if b, err := json.Marshal(j.Input); err == nil {
			_ = json.Unmarshal(b, &q)
			j.retry = a.createPipelineHandler(q)
		}
	}
	if j.retry != nil {
		j.retry(j)
	}
}

func (a *App) retryJob(jobID string) (*Job, error) {
	a.mu.Lock()
	old := a.jobs[jobID]
	if old == nil {
		a.mu.Unlock()
		return nil, errors.New("job not found")
	}
	if old.Status != "failed" && old.Status != "canceled" {
		a.mu.Unlock()
		return nil, fmt.Errorf("only failed or canceled jobs can be retried (status=%s)", old.Status)
	}

	// A previous version created a new record for every retry. Collapse those
	// terminal duplicates before retrying, and never start a second copy when
	// the same input is already queued or running.
	oldInput, _ := json.Marshal(old.Input)
	activeDuplicate := (*Job)(nil)
	removeIDs := make(map[string]bool)
	for oid, candidate := range a.jobs {
		if oid == jobID || candidate == nil || candidate.Kind != old.Kind {
			continue
		}
		candidateInput, _ := json.Marshal(candidate.Input)
		if !bytes.Equal(oldInput, candidateInput) {
			continue
		}
		if candidate.Status == "queued" || candidate.Status == "running" {
			activeDuplicate = candidate
		} else if candidate.Status == "failed" || candidate.Status == "canceled" {
			removeIDs[oid] = true
		}
	}
	if activeDuplicate != nil {
		removeIDs[jobID] = true
		for oid := range removeIDs {
			delete(a.jobs, oid)
		}
		a.removeJobIDsLocked(removeIDs)
		a.mu.Unlock()
		a.saveJobs()
		return activeDuplicate, nil
	}

	// Replace the terminal record in-place from the queue's point of view.
	// Creating a second record made every retry look like a duplicate task and
	// caused "retry all" to double the visible queue. A fresh Job object keeps
	// a canceled worker from writing its final state into the retried attempt.
	ctx, cancel := context.WithCancel(context.Background())
	j := &Job{
		ID:         id(),
		Kind:       old.Kind,
		Status:     "queued",
		Step:       "排队中",
		Created:    time.Now(),
		Input:      old.Input,
		ctx:        ctx,
		cancelFunc: cancel,
	}
	for i, oid := range a.order {
		if oid == jobID {
			a.order[i] = j.ID
			break
		}
	}
	for oid := range removeIDs {
		delete(a.jobs, oid)
	}
	a.removeJobIDsLocked(removeIDs)
	delete(a.jobs, jobID)
	a.jobs[j.ID] = j
	a.mu.Unlock()
	a.saveJobs()
	a.dispatchJob(j)
	return j, nil
}

func (a *App) removeJobIDsLocked(ids map[string]bool) {
	if len(ids) == 0 {
		return
	}
	filtered := a.order[:0]
	for _, oid := range a.order {
		if !ids[oid] {
			filtered = append(filtered, oid)
		}
	}
	a.order = filtered
}

func jsonResp(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func decode(r *http.Request, v any) error {
	if r.Body == nil {
		return errors.New("empty body")
	}
	return json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(v)
}

// Subprocess runner with live log capture. onLine is called while the process
// is running, which lets the queue expose progress before a long command ends.
func runCmdProgress(ctx context.Context, bin string, args []string, onLine func(string)) (string, error) {
	c := exec.CommandContext(ctx, bin, args...)
	c.Env = os.Environ()
	capture := &progressCapture{buffer: &limitedBuffer{max: 128 << 10}, onLine: onLine}
	c.Stdout = capture
	c.Stderr = capture
	err := c.Run()
	capture.flush()
	output := strings.TrimSpace(capture.String())
	if err != nil {
		return output, fmt.Errorf("%s: %w: %s", bin, err, output)
	}
	return output, nil
}

type progressCapture struct {
	mu      sync.Mutex
	buffer  *limitedBuffer
	pending string
	onLine  func(string)
}

func (c *progressCapture) Write(p []byte) (int, error) {
	c.mu.Lock()
	_, _ = c.buffer.Write(p)
	c.pending += string(p)
	parts := strings.Split(c.pending, "\n")
	c.pending = parts[len(parts)-1]
	lines := append([]string(nil), parts[:len(parts)-1]...)
	c.mu.Unlock()
	if c.onLine != nil {
		for _, line := range lines {
			c.onLine(strings.TrimSuffix(line, "\r"))
		}
	}
	return len(p), nil
}

func (c *progressCapture) flush() {
	c.mu.Lock()
	line := strings.TrimSuffix(c.pending, "\r")
	c.pending = ""
	c.mu.Unlock()
	if line != "" && c.onLine != nil {
		c.onLine(line)
	}
}

func (c *progressCapture) String() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.buffer.String()
}

func runCmd(ctx context.Context, bin string, args []string) (string, error) {
	return runCmdProgress(ctx, bin, args, nil)
}

func parseSpeedBytes(raw string) int64 {
	raw = strings.TrimSpace(strings.TrimSuffix(raw, "/s"))
	if raw == "" || raw == "N/A" {
		return 0
	}
	multiplier := float64(1)
	for _, unit := range []struct {
		suffix string
		value  float64
	}{{"GiB", 1 << 30}, {"MiB", 1 << 20}, {"KiB", 1 << 10}, {"GB", 1e9}, {"MB", 1e6}, {"KB", 1e3}, {"B", 1}} {
		if strings.HasSuffix(raw, unit.suffix) {
			raw = strings.TrimSpace(strings.TrimSuffix(raw, unit.suffix))
			multiplier = unit.value
			break
		}
	}
	n, _ := strconv.ParseFloat(strings.TrimSpace(raw), 64)
	return int64(n * multiplier)
}

func parseETASeconds(raw string) int64 {
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "NA" || raw == "Unknown" {
		return 0
	}
	if n, err := strconv.ParseInt(raw, 10, 64); err == nil {
		return n
	}
	if strings.HasSuffix(raw, "s") {
		n, _ := strconv.ParseInt(strings.TrimSuffix(raw, "s"), 10, 64)
		return n
	}
	if strings.HasSuffix(raw, "m") {
		n, _ := strconv.ParseInt(strings.TrimSuffix(raw, "m"), 10, 64)
		return n * 60
	}
	if strings.HasSuffix(raw, "h") {
		n, _ := strconv.ParseInt(strings.TrimSuffix(raw, "h"), 10, 64)
		return n * 3600
	}
	var total int64
	for _, part := range strings.Split(raw, ":") {
		n, err := strconv.ParseInt(strings.TrimSpace(part), 10, 64)
		if err != nil {
			return 0
		}
		total = total*60 + n
	}
	return total
}

var (
	ytdlpProgressRE = regexp.MustCompile(`^download:\s*([^|]+)\|([^|]*)\|([^|]*)\|([^|]*)\|([^|]*)\|(.*)$`)
	percentRE       = regexp.MustCompile(`([0-9]+(?:\.[0-9]+)?)%`)
	ariaProgressRE  = regexp.MustCompile(`(?:^|\s)(\d+(?:\.\d+)?)%.*?DL:([^,\s]+(?:\s*[KMGT]i?B)?).*?ETA:([^\s,]+)`)
	ffmpegTimeRE    = regexp.MustCompile(`(?:time|out_time)=([0-9:.]+)`)
)

func (a *App) progressLine(j *Job, phase, line string) {
	p := JobProgress{Detail: phase}
	if m := ytdlpProgressRE.FindStringSubmatch(strings.TrimSpace(line)); len(m) == 7 {
		p.Percent, _ = strconv.ParseFloat(strings.TrimSpace(strings.TrimSuffix(m[1], "%")), 64)
		p.Downloaded, _ = strconv.ParseInt(strings.TrimSpace(m[2]), 10, 64)
		p.Total, _ = strconv.ParseInt(strings.TrimSpace(m[3]), 10, 64)
		p.Speed = parseSpeedBytes(m[4])
		p.ETASeconds = parseETASeconds(m[5])
		p.Current = strings.TrimSpace(m[6])
		if p.Total > 0 && p.Percent == 0 {
			p.Percent = float64(p.Downloaded) * 100 / float64(p.Total)
		}
		a.setProgress(j, p)
		return
	}
	if m := ariaProgressRE.FindStringSubmatch(line); len(m) == 4 {
		p.Percent, _ = strconv.ParseFloat(m[1], 64)
		p.Speed = parseSpeedBytes(m[2])
		p.ETASeconds = parseETASeconds(m[3])
		a.setProgress(j, p)
		return
	}
	if m := percentRE.FindStringSubmatch(line); len(m) == 2 {
		p.Percent, _ = strconv.ParseFloat(m[1], 64)
	}
	if m := ffmpegTimeRE.FindStringSubmatch(line); len(m) == 2 {
		p.Current = "处理到 " + m[1]
	}
	if strings.HasPrefix(strings.TrimSpace(line), "[ffmpeg]") {
		p.Current = strings.TrimSpace(line)
	}
	if p.Percent > 0 || p.Current != "" || strings.Contains(strings.ToLower(line), "speed=") {
		a.setProgress(j, p)
	}
}

type limitedBuffer struct {
	b   bytes.Buffer
	max int
}

func (b *limitedBuffer) String() string { return b.b.String() }

func (b *limitedBuffer) Write(p []byte) (int, error) {
	if b.b.Len() < b.max {
		_, _ = b.b.Write(p[:min(len(p), b.max-b.b.Len())])
	}
	return len(p), nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func trim200(s string) string {
	s = strings.TrimSpace(s)
	for len([]rune(s)) > 200 {
		_, n := utf8.DecodeLastRuneInString(s)
		s = s[:len(s)-n]
	}
	return s
}

// YouTube Downloader
type youtubeReq struct {
	URL           string `json:"url"`
	SubLangs      string `json:"sub_langs"`
	Quality       string `json:"quality"`     // "best", "1080p", "720p", "audio_only"
	AutoUpload    bool   `json:"auto_upload"` // Chained pipeline upload
	Tid           string `json:"tid"`
	Tags          string `json:"tags"`
	Translate     bool   `json:"translate"`
	SplitChapters bool   `json:"split_chapters"` // 段落自动分P
	BurnSubs      bool   `json:"burn_subs"`      // 显式为 true 才压制；默认复用 YouTube 字幕
}

func validYouTube(s string) bool {
	u, e := url.Parse(s)
	if e != nil {
		return false
	}
	h := strings.ToLower(strings.TrimSuffix(u.Hostname(), "."))
	return h == "youtube.com" || strings.HasSuffix(h, ".youtube.com") || h == "youtu.be"
}

func isPlaylistURL(s string) bool {
	u, err := url.Parse(s)
	if err != nil {
		return false
	}
	q := u.Query()
	return q.Get("list") != "" || strings.Contains(u.Path, "/playlist")
}

func (a *App) chapterSplitDecision(ctx context.Context, rawURL, cookiePath string, requested bool) (bool, string) {
	if !requested || isPlaylistURL(rawURL) {
		return requested, ""
	}
	args := []string{
		"--dump-single-json",
		"--skip-download",
		"--no-playlist",
		"--no-warnings",
		"--no-plugin-dirs",
		rawURL,
	}
	if cookiePath != "" {
		args = append(args[:len(args)-1], "--cookies", cookiePath, rawURL)
	}
	out, err := runCmd(ctx, a.cfg.YTDLP, args)
	if err != nil {
		return requested, "[分P策略] 无法读取视频时长，保留用户选择"
	}
	var meta struct {
		Duration float64 `json:"duration"`
	}
	if json.Unmarshal([]byte(out), &meta) != nil || meta.Duration <= 0 {
		return requested, "[分P策略] 未获取到有效时长，保留用户选择"
	}
	if meta.Duration < 30*60 {
		return false, fmt.Sprintf("[分P策略] 视频时长 %s，小于 30 分钟，自动关闭章节分P", formatDurationSeconds(meta.Duration))
	}
	return requested, fmt.Sprintf("[分P策略] 视频时长 %s，按用户选择处理章节分P", formatDurationSeconds(meta.Duration))
}

func formatDurationSeconds(seconds float64) string {
	whole := int64(seconds)
	if whole < 60 {
		return fmt.Sprintf("%ds", whole)
	}
	minutes := whole / 60
	if minutes < 60 {
		return fmt.Sprintf("%dm %ds", minutes, whole%60)
	}
	return fmt.Sprintf("%dh %dm", minutes/60, minutes%60)
}

func (a *App) youtube(w http.ResponseWriter, r *http.Request) {
	var q youtubeReq
	if decode(r, &q) != nil || !validYouTube(q.URL) {
		jsonResp(w, 400, map[string]string{"error": "valid YouTube URL required"})
		return
	}
	j := a.add("youtube", q)
	a.dispatchJob(j)
	jsonResp(w, 202, j)
}

func (a *App) createYoutubeHandler(q youtubeReq) func(*Job) {
	return func(nj *Job) {
		go func() {
			d := filepath.Join(a.cfg.DataDir, "youtube", nj.ID)
			_ = os.MkdirAll(d, 0750)

			var baseFiles []string
			var targetUploadFiles []string
			var videoFiles []string
			var mainVideoFile string
			var totalLogs string
			var downloadErr error

			// Stage 1: Download stage (acquires downloadSlots)
			func() {
				if err := a.acquireSlot(nj.ctx, a.downloadSlots); err != nil {
					downloadErr = err
					return
				}
				defer func() {
					<-a.downloadSlots
					runtime.GC()
					debug.FreeOSMemory()
				}()

				if err := a.ensureSafeMemory(nj.ctx); err != nil {
					downloadErr = err
					return
				}

				a.set(nj, "running", "", nil, "")
				a.setStep(nj, "下载媒体中")
				cookiePath, cleanup, _ := prepareCookies(a.cfg.Cookies, d)
				defer cleanup()

				langs := strings.TrimSpace(q.SubLangs)
				if langs == "" {
					langs = "zh-Hans,zh,en,zh-Hant"
				}
				isPlaylist := isPlaylistURL(q.URL)
				splitChapters, splitLog := a.chapterSplitDecision(nj.ctx, q.URL, cookiePath, q.SplitChapters)
				if splitLog != "" {
					totalLogs += splitLog + "\n"
				}
				args := []string{
					"--ignore-errors",
					"--no-abort-on-error",
					"--buffer-size", "16K",
					"--http-chunk-size", "10M",
					"--concurrent-fragments", "1",
					"--no-cache-dir",
					"--no-plugin-dirs",
					"--newline",
					"--progress-template", "download:%(progress._percent_str)s|%(progress.downloaded_bytes)s|%(progress.total_bytes)s|%(progress.speed)s|%(progress.eta)s|%(info.title)s",
					"--postprocessor-args", "ffmpeg:-threads 1",
					"--extractor-args", "youtube:player_client=android,ios,web,tv_downgraded,default",
				}
				if isPlaylist {
					args = append(args, "--yes-playlist")
				} else {
					args = append(args, "--no-playlist")
				}
				if cookiePath != "" {
					args = append(args, "--cookies", cookiePath)
				}
				if langs != "none" && langs != "no" {
					args = append(args, "--write-subs", "--sub-langs", langs, "--embed-subs")
				}

				switch q.Quality {
				case "audio_only":
					args = append(args, "-x", "--audio-format", "mp3")
				case "720p":
					args = append(args, "-f", "22/bv*[height<=720][ext=mp4]+ba[ext=m4a]/b[height<=720][ext=mp4]/bv*[height<=720]+ba/b/18")
				case "1080p":
					args = append(args, "-f", "bv*[height<=1080][ext=mp4]+ba[ext=m4a]/b[height<=1080][ext=mp4]/bv*[height<=1080]+ba/b/22/18")
				default:
					args = append(args, "-f", "bv*[ext=mp4]+ba[ext=m4a]/bv*+ba/b[ext=mp4]/b/22/18")
				}

				if isPlaylist {
					if splitChapters {
						args = append(args,
							"--split-chapters",
							"-o", "chapter:"+filepath.Join(d, "P%(playlist_index|1)02d - C%(section_number)02d. %(section_title)s.%(ext)s"),
						)
					}
					args = append(args,
						"--write-thumbnail",
						"--write-description",
						"--embed-metadata",
						"--merge-output-format", "mp4",
						"-o", filepath.Join(d, "P%(playlist_index|1)02d. %(title)s [%(id)s].%(ext)s"),
						q.URL,
					)
				} else {
					if splitChapters {
						args = append(args,
							"--split-chapters",
							"-o", "chapter:"+filepath.Join(d, "%(title)s - P%(section_number)02d. %(section_title)s.%(ext)s"),
						)
					}
					args = append(args,
						"--write-thumbnail",
						"--write-description",
						"--embed-metadata",
						"--merge-output-format", "mp4",
						"-o", filepath.Join(d, "%(title)s [%(id)s].%(ext)s"),
						q.URL,
					)
				}

				ytLogs, err := runCmdProgress(nj.ctx, a.cfg.YTDLP, args, func(line string) { a.progressLine(nj, "YouTube 下载", line) })
				totalLogs = ytLogs
				downloadErr = err

				files, _ := filepath.Glob(filepath.Join(d, "*"))
				sort.Strings(files)
				var chapterFiles []string
				for _, f := range files {
					name := filepath.Base(f)
					baseFiles = append(baseFiles, name)
					ext := strings.ToLower(filepath.Ext(name))
					if ext == ".mp4" || ext == ".mkv" || ext == ".webm" || ext == ".mp3" {
						if strings.Contains(name, " - P") || strings.Contains(name, " - C") {
							chapterFiles = append(chapterFiles, f)
						} else {
							videoFiles = append(videoFiles, f)
						}
					}
				}
				targetUploadFiles = videoFiles
				if len(chapterFiles) > 0 {
					targetUploadFiles = chapterFiles
				}

				convertVttToSrtAndBcc(d)
				if q.BurnSubs && len(targetUploadFiles) > 0 {
					a.setStep(nj, "正在压制中英硬字幕...")
					a.setProgress(nj, JobProgress{Detail: "ffmpeg 字幕压制"})
					burned, bLogs, _ := burnSubtitlesToVideos(nj.ctx, d, targetUploadFiles, func(line string) { a.progressLine(nj, "ffmpeg 字幕压制", line) })
					targetUploadFiles = burned
					totalLogs += "\n[字幕压制日志]\n" + bLogs
				}

				if len(targetUploadFiles) > 0 {
					mainVideoFile = targetUploadFiles[0]
				}
			}()

			outMap := map[string]any{
				"dir":         d,
				"files":       baseFiles,
				"video_file":  mainVideoFile,
				"video_files": targetUploadFiles,
				"is_multi_p":  len(targetUploadFiles) > 1,
			}

			if downloadErr != nil {
				if errors.Is(downloadErr, context.Canceled) {
					a.set(nj, "canceled", "已取消", outMap, totalLogs)
				} else {
					a.set(nj, "failed", downloadErr.Error(), outMap, totalLogs)
				}
				return
			}

			downBytes := calcFilesSize(append(targetUploadFiles, videoFiles...))
			if downBytes == 0 {
				downBytes = calcDirSize(d)
			}
			a.recordDownload(downBytes)

			// If AutoUpload requested, enter Stage 2 (acquires uploadSlots)
			if q.AutoUpload && len(targetUploadFiles) > 0 {
				var uploadOut map[string]any
				var uploadLogs string
				var uploadErr error

				func() {
					if err := a.acquireSlot(nj.ctx, a.uploadSlots); err != nil {
						uploadErr = err
						return
					}
					defer func() {
						<-a.uploadSlots
						runtime.GC()
						debug.FreeOSMemory()
					}()

					if err := a.ensureSafeMemory(nj.ctx); err != nil {
						uploadErr = err
						return
					}

					a.setStep(nj, "B站投稿中")
					a.setProgress(nj, JobProgress{Detail: "B站上传"})
					uploadOut, uploadLogs, uploadErr = a.executeBiliupUpload(nj.ctx, uploadReq{
						Files:     targetUploadFiles,
						File:      mainVideoFile,
						Translate: q.Translate,
						Tid:       q.Tid,
						Tag:       q.Tags,
						Parts:     true,
						Source:    q.URL,
						Progress:  func(line string) { a.progressLine(nj, "B站上传", line) },
					})
					totalLogs += "\n--- BILIUP UPLOAD LOGS ---\n" + uploadLogs
				}()

				outMap["upload"] = uploadOut

				if uploadErr != nil {
					if errors.Is(uploadErr, context.Canceled) {
						a.set(nj, "canceled", "已取消", outMap, totalLogs)
					} else {
						a.set(nj, "failed", uploadErr.Error(), outMap, totalLogs)
					}
					return
				}

				upBytes := calcFilesSize(targetUploadFiles)
				if upBytes == 0 {
					upBytes = downBytes
				}
				a.recordUpload(upBytes)
				a.recordPipelineSuccess()
				if q.Translate {
					a.recordAiTrans()
				}

				// Auto-Clean downloaded raw video files after successful Bilibili upload
				freed := purgeVideoFiles(append(targetUploadFiles, videoFiles...)...)
				if freed > 0 {
					totalLogs += fmt.Sprintf("\n[自动空间清理] B站投稿成功，已自动删除原视频文件，释放磁盘空间: %s\n", formatBytes(freed))
					outMap["cleaned_disk"] = formatBytes(freed)
				}
			}

			a.set(nj, "done", "", outMap, totalLogs)
		}()
	}
}

func purgeVideoFiles(files ...string) int64 {
	var freed int64
	seen := make(map[string]bool)
	for _, f := range files {
		if f == "" || seen[f] {
			continue
		}
		seen[f] = true
		if fi, err := os.Stat(f); err == nil {
			freed += fi.Size()
			_ = os.Remove(f)
		}
	}
	return freed
}

func (a *App) purgeManagedVideoFiles(files ...string) int64 {
	root := filepath.Clean(a.cfg.DataDir)
	managed := make([]string, 0, len(files))
	for _, file := range files {
		clean := filepath.Clean(file)
		rel, err := filepath.Rel(root, clean)
		if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || filepath.IsAbs(rel) {
			continue
		}
		managed = append(managed, clean)
	}
	return purgeVideoFiles(managed...)
}

// Magnet Downloader
type magnetReq struct {
	Magnet     string `json:"magnet"`
	URL        string `json:"url"`
	AutoUpload bool   `json:"auto_upload"`
	Tid        string `json:"tid"`
	Tags       string `json:"tags"`
	Translate  bool   `json:"translate"`
}

func validTorrentOrMagnet(s string) bool {
	s = strings.TrimSpace(s)
	lower := strings.ToLower(s)
	if strings.HasPrefix(lower, "magnet:") {
		u, err := url.Parse(s)
		return err == nil && strings.EqualFold(u.Scheme, "magnet") && u.Query().Get("xt") != ""
	}
	return strings.HasPrefix(lower, "http://") || strings.HasPrefix(lower, "https://")
}

func isDeadSeedOutput(logs string) bool {
	lower := strings.ToLower(logs)
	markers := []string{
		"no seed",
		"no peer",
		"download speed is 0",
		"download speed of 0",
		"bt-stop-timeout",
		"number of seeders: 0",
	}
	for _, marker := range markers {
		if strings.Contains(lower, marker) {
			return true
		}
	}
	return false
}

func (a *App) runMagnet(ctx context.Context, args []string) (string, error) {
	return a.runMagnetProgress(ctx, args, nil)
}

func (a *App) runMagnetProgress(ctx context.Context, args []string, callbacks ...func(string)) (string, error) {
	var onLine func(string)
	if len(callbacks) > 0 {
		onLine = callbacks[0]
	}
	if a.cfg.MagnetTimeout <= 0 {
		return runCmdProgress(ctx, a.cfg.Aria2, args, onLine)
	}
	magnetCtx, cancel := context.WithTimeout(ctx, a.cfg.MagnetTimeout)
	defer cancel()
	logs, err := runCmdProgress(magnetCtx, a.cfg.Aria2, args, onLine)
	if err == nil {
		return logs, nil
	}
	if errors.Is(magnetCtx.Err(), context.DeadlineExceeded) {
		return logs, fmt.Errorf("magnet_timeout: BT 下载超过 %s", a.cfg.MagnetTimeout)
	}
	if isDeadSeedOutput(logs) {
		return logs, fmt.Errorf("dead_seed: 未发现可用做种或下载速度持续为 0")
	}
	return logs, err
}

func (a *App) magnet(w http.ResponseWriter, r *http.Request) {
	var q magnetReq
	if decode(r, &q) != nil {
		jsonResp(w, 400, map[string]string{"error": "JSON required"})
		return
	}
	m := q.Magnet
	if m == "" {
		m = q.URL
	}
	if !validTorrentOrMagnet(m) {
		jsonResp(w, 400, map[string]string{"error": "valid magnet: URI or torrent URL required"})
		return
	}
	j := a.add("magnet", q)
	a.dispatchJob(j)
	jsonResp(w, 202, j)
}

func (a *App) createMagnetHandler(q magnetReq) func(*Job) {
	return func(nj *Job) {
		go func() {
			m := q.Magnet
			if m == "" {
				m = q.URL
			}
			d := filepath.Join(a.cfg.DataDir, "magnet", nj.ID)
			_ = os.MkdirAll(d, 0750)

			var baseFiles []string
			var videoFiles []string
			var videoFile string
			var totalLogs string
			var downloadErr error

			// Stage 1: Magnet Download (acquires downloadSlots)
			func() {
				if err := a.acquireSlot(nj.ctx, a.downloadSlots); err != nil {
					downloadErr = err
					return
				}
				defer func() {
					<-a.downloadSlots
					runtime.GC()
					debug.FreeOSMemory()
				}()

				if err := a.ensureSafeMemory(nj.ctx); err != nil {
					downloadErr = err
					return
				}

				a.set(nj, "running", "", nil, "")
				a.setStep(nj, "磁力下载中")
				btTrackers := "udp://tracker.opentrackr.org:1337/announce,udp://open.tracker.cl:1337/announce,udp://tracker.openbittorrent.com:6969/announce,http://tracker.openbittorrent.com:80/announce,udp://opentracker.i2p.rocks:6969/announce,udp://open.demonii.com:1337/announce"
				magLogs, err := a.runMagnetProgress(nj.ctx, []string{
					"--dir=" + d,
					"--continue=true",
					"--allow-overwrite=false",
					"--seed-time=0",
					"--file-allocation=none",
					"--disk-cache=16M",
					"--timeout=30",
					"--connect-timeout=15",
					"--bt-tracker-connect-timeout=15",
					"--bt-tracker-timeout=20",
					"--listen-port=" + a.cfg.BTListenPort,
					"--max-connection-per-server=4",
					"--bt-max-peers=60",
					"--max-concurrent-downloads=1",
					"--enable-dht=true",
					"--enable-peer-exchange=true",
					"--bt-enable-lpd=true",
					"--follow-torrent=mem",
					"--bt-stop-timeout=300",
					"--summary-interval=1",
					"--bt-tracker=" + btTrackers,
					m,
				}, func(line string) { a.progressLine(nj, "BT 下载", line) })
				totalLogs = magLogs
				downloadErr = err

				_ = filepath.Walk(d, func(p string, info os.FileInfo, err error) error {
					if err != nil || info.IsDir() {
						return nil
					}
					name := info.Name()
					baseFiles = append(baseFiles, name)
					ext := strings.ToLower(filepath.Ext(name))
					if ext == ".mp4" || ext == ".mkv" || ext == ".avi" || ext == ".webm" || ext == ".mp3" {
						videoFiles = append(videoFiles, p)
						if videoFile == "" || ext == ".mp4" {
							videoFile = p
						}
					}
					return nil
				})
				sort.Strings(videoFiles)
			}()

			outMap := map[string]any{"dir": d, "files": baseFiles, "video_file": videoFile, "video_files": videoFiles, "is_multi_p": len(videoFiles) > 1}

			if downloadErr != nil {
				if errors.Is(downloadErr, context.Canceled) {
					a.set(nj, "canceled", "已取消", outMap, totalLogs)
				} else {
					a.set(nj, "failed", downloadErr.Error(), outMap, totalLogs)
				}
				return
			}

			downBytes := int64(0)
			if len(videoFiles) > 0 {
				downBytes = calcFilesSize(videoFiles)
			}
			if downBytes == 0 {
				downBytes = calcDirSize(d)
			}
			a.recordDownload(downBytes)

			// If AutoUpload requested, enter Stage 2 (acquires uploadSlots)
			if q.AutoUpload && len(videoFiles) > 0 {
				var uploadOut map[string]any
				var uploadLogs string
				var uploadErr error

				func() {
					if err := a.acquireSlot(nj.ctx, a.uploadSlots); err != nil {
						uploadErr = err
						return
					}
					defer func() {
						<-a.uploadSlots
						runtime.GC()
						debug.FreeOSMemory()
					}()

					if err := a.ensureSafeMemory(nj.ctx); err != nil {
						uploadErr = err
						return
					}

					a.setStep(nj, "B站投稿中")
					a.setProgress(nj, JobProgress{Detail: "B站上传"})
					uploadOut, uploadLogs, uploadErr = a.executeBiliupUpload(nj.ctx, uploadReq{
						File:      videoFile,
						Files:     videoFiles,
						Translate: q.Translate,
						Tid:       q.Tid,
						Tag:       q.Tags,
						Parts:     len(videoFiles) > 1,
						Source:    m,
						Progress:  func(line string) { a.progressLine(nj, "B站上传", line) },
					})
					totalLogs += "\n--- BILIUP UPLOAD LOGS ---\n" + uploadLogs
				}()

				outMap["upload"] = uploadOut

				if uploadErr != nil {
					if errors.Is(uploadErr, context.Canceled) {
						a.set(nj, "canceled", "已取消", outMap, totalLogs)
					} else {
						a.set(nj, "failed", uploadErr.Error(), outMap, totalLogs)
					}
					return
				}

				upBytes := calcFilesSize(videoFiles)
				if upBytes == 0 {
					upBytes = downBytes
				}
				a.recordUpload(upBytes)
				a.recordPipelineSuccess()
				if q.Translate {
					a.recordAiTrans()
				}

				freed := purgeVideoFiles(videoFiles...)
				if freed > 0 {
					totalLogs += fmt.Sprintf("\n[自动空间清理] B站投稿成功，已自动清除原视频文件，释放磁盘空间: %s\n", formatBytes(freed))
					outMap["cleaned_disk"] = formatBytes(freed)
				}
			}

			a.set(nj, "done", "", outMap, totalLogs)
		}()
	}
}

// Biliup Upload Request & Helpers
type uploadReq struct {
	File        string       `json:"file"`
	Files       []string     `json:"files"`
	Title       string       `json:"title"`
	Description string       `json:"description"`
	Cover       string       `json:"cover"`
	Tag         string       `json:"tag"`
	Tid         string       `json:"tid"`
	Limit       string       `json:"limit"`
	Source      string       `json:"source"`
	Translate   bool         `json:"translate"`
	Parts       bool         `json:"parts"`
	Progress    func(string) `json:"-"`
}

type BiliCodeResult struct {
	Code    int
	Message string
	BVID    string
	RawLogs string
}

type biliRepairAction string

const (
	biliRepairSuccess   biliRepairAction = "success"
	biliRepairStop      biliRepairAction = "stop"
	biliRepairTitle     biliRepairAction = "sanitize_title"
	biliRepairDesc      biliRepairAction = "sanitize_description"
	biliRepairTags      biliRepairAction = "fallback_tags"
	biliRepairTID       biliRepairAction = "fallback_tid"
	biliRepairCover     biliRepairAction = "remove_cover"
	biliRepairSwitch    biliRepairAction = "switch_endpoint"
	biliRepairRateLimit biliRepairAction = "rate_limited"
	biliRepairUnknown   biliRepairAction = "unknown"
)

// biliRepairActionFor is deliberately pure so every codestatus branch can be
// tested without touching Bilibili or creating a real submission.
func biliRepairActionFor(code int) biliRepairAction {
	switch code {
	case 0:
		return biliRepairSuccess
	case -101, 21016, 21017, 21018, 21070, 21071, 21564:
		return biliRepairStop
	case 21020, 21021, 21022:
		return biliRepairTitle
	case 21023, 21024, 21025:
		return biliRepairDesc
	case 21030, 21031, 21033:
		return biliRepairTags
	case 21040, 21041, 21042:
		return biliRepairTID
	case 21050, 21051, 21052:
		return biliRepairCover
	case 21138:
		// biliup's own Python implementation treats this as web-submit
		// incompatibility and falls back to the client endpoint.
		return biliRepairSwitch
	case 601:
		return biliRepairRateLimit
	default:
		return biliRepairUnknown
	}
}

func parseBiliupOutput(logs string) BiliCodeResult {
	res := BiliCodeResult{Code: -1, RawLogs: logs}
	reBV := regexp.MustCompile(`BV[a-zA-Z0-9]{10}`)
	if m := reBV.FindString(logs); m != "" {
		res.BVID = m
	}
	reCode := regexp.MustCompile(`code:\s*(-?\d+)`)
	if m := reCode.FindStringSubmatch(logs); len(m) > 1 {
		if c, err := strconv.Atoi(m[1]); err == nil {
			res.Code = c
		}
	}
	reMsg := regexp.MustCompile(`message:\s*"([^"]+)"`)
	if m := reMsg.FindStringSubmatch(logs); len(m) > 1 {
		res.Message = m[1]
	}
	if res.Code == 0 || res.BVID != "" || strings.Contains(logs, "投稿成功") {
		res.Code = 0
		if res.Message == "" {
			res.Message = "OK"
		}
	}
	return res
}

func sanitizeBiliTitle(s string) string {
	s = strings.TrimSpace(s)
	s = strings.ReplaceAll(s, "\n", " ")
	s = strings.ReplaceAll(s, "\r", " ")
	s = strings.ReplaceAll(s, "\t", " ")
	runes := []rune(s)
	if len(runes) > 80 {
		runes = runes[:80]
	}
	return strings.TrimSpace(string(runes))
}

func sanitizeBiliDesc(s string) string {
	s = strings.TrimSpace(s)
	runes := []rune(s)
	if len(runes) > 200 {
		runes = runes[:200]
	}
	return strings.TrimSpace(string(runes))
}

func sanitizeBiliTags(tags string) string {
	raw := strings.Split(tags, ",")
	var cleaned []string
	seen := make(map[string]bool)
	for _, t := range raw {
		t = strings.TrimSpace(t)
		t = strings.ReplaceAll(t, " ", "")
		runes := []rune(t)
		if len(runes) > 15 {
			runes = runes[:15]
		}
		t = string(runes)
		if t != "" && !seen[t] {
			seen[t] = true
			cleaned = append(cleaned, t)
		}
		if len(cleaned) >= 10 {
			break
		}
	}
	if len(cleaned) == 0 {
		return "科技,软件应用,教程"
	}
	return strings.Join(cleaned, ",")
}

func (a *App) sanitizeBiliCover(ctx context.Context, coverPath string, callbacks ...func(string)) string {
	if coverPath == "" {
		return ""
	}
	fi, err := os.Stat(coverPath)
	if err != nil {
		return ""
	}
	ext := strings.ToLower(filepath.Ext(coverPath))
	if (ext == ".jpg" || ext == ".jpeg") && fi.Size() <= 1500*1024 {
		return coverPath
	}

	fixedCover := filepath.Join(filepath.Dir(coverPath), "cover_optimized.jpg")
	args := []string{
		"-y", "-i", coverPath,
		"-vf", "scale=960:600:force_original_aspect_ratio=decrease,pad=960:600:(ow-iw)/2:(oh-ih)/2",
		"-q:v", "2",
		fixedCover,
	}
	var onLine func(string)
	if len(callbacks) > 0 {
		onLine = callbacks[0]
	}
	if _, err := runCmdProgress(ctx, "ffmpeg", args, onLine); err == nil {
		if cfi, err := os.Stat(fixedCover); err == nil && cfi.Size() > 0 {
			return fixedCover
		}
	}
	return coverPath
}

func partTitleFromFile(file string) string {
	name := strings.TrimSuffix(filepath.Base(file), filepath.Ext(file))
	name = strings.TrimSpace(name)
	if name == "" {
		return "分P"
	}
	return name
}

func safePartFilename(title string, index int, ext string) string {
	title = strings.TrimSpace(strings.NewReplacer("/", "-", "\\", "-", ":", "：", "\x00", "").Replace(title))
	if title == "" {
		title = "分P"
	}
	runes := []rune(title)
	if len(runes) > 150 {
		runes = runes[:150]
	}
	return fmt.Sprintf("P%02d - %s%s", index+1, strings.TrimSpace(string(runes)), ext)
}

// prepareTranslatedPartFiles creates lightweight symlinks whose basenames are
// translated. biliup uses each uploaded basename as the Bilibili P title, while
// --title only controls the parent稿件 title. Keeping the originals untouched
// lets retry/cleanup continue to operate on the real media files.
func (a *App) prepareTranslatedPartFiles(ctx context.Context, files []string) ([]string, func(), int, string) {
	translated := append([]string(nil), files...)
	tmpDir := ""
	translatedCount := 0
	var logs strings.Builder
	cleanup := func() {
		if tmpDir != "" {
			_ = os.RemoveAll(tmpDir)
		}
	}

	for i, file := range files {
		partTitle := partTitleFromFile(file)
		result, err := a.aiEnhanceMetadata(ctx, partTitle, "")
		if err != nil || result == nil || strings.TrimSpace(result.Title) == "" {
			continue
		}
		translatedTitle := sanitizeBiliTitle(result.Title)
		if translatedTitle == "" || translatedTitle == partTitle {
			continue
		}
		if tmpDir == "" {
			var mkErr error
			tmpDir, mkErr = os.MkdirTemp(filepath.Dir(file), ".y2b-translated-parts-")
			if mkErr != nil {
				tmpDir = ""
				continue
			}
		}
		target := filepath.Join(tmpDir, safePartFilename(translatedTitle, i, filepath.Ext(file)))
		if err := os.Symlink(file, target); err != nil {
			continue
		}
		translated[i] = target
		translatedCount++
		logs.WriteString(fmt.Sprintf("[分P标题翻译] P%d: %s -> %s\n", i+1, partTitle, translatedTitle))
	}
	return translated, cleanup, translatedCount, logs.String()
}

func (a *App) executeBiliupUpload(ctx context.Context, q uploadReq) (map[string]any, string, error) {
	if a.cfg.UploadTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, a.cfg.UploadTimeout)
		defer cancel()
	}
	files := q.Files
	if len(files) == 0 && q.File != "" {
		files = []string{q.File}
	}
	if len(files) == 0 {
		return nil, "", errors.New("no files provided for upload")
	}
	uploadFiles := files
	cleanupTranslated := func() {}
	translatedCount := 0
	partTranslateLogs := ""
	if q.Translate {
		uploadFiles, cleanupTranslated, translatedCount, partTranslateLogs = a.prepareTranslatedPartFiles(ctx, files)
		defer cleanupTranslated()
	}

	desc := sanitizeBiliDesc(q.Description)
	if desc == "" {
		desc = sanitizeBiliDesc(adjacentText(files[0], ".description"))
	}

	title := sanitizeBiliTitle(q.Title)
	if title == "" {
		baseName := strings.TrimSuffix(filepath.Base(files[0]), filepath.Ext(files[0]))
		reSplit := regexp.MustCompile(`\s*-\s*[PC]\d+.*$`)
		if m := reSplit.ReplaceAllString(baseName, ""); m != "" {
			baseName = m
		}
		title = sanitizeBiliTitle(baseName)
	}

	tags := sanitizeBiliTags(q.Tag)
	if tags == "" {
		tags = sanitizeBiliTags(a.cfg.DefaultTags)
	}

	tid := q.Tid
	if tid == "" {
		tid = "188" // 科技 - 软件应用
	}

	if q.Translate && a.cfg.DeepSeekKey != "" {
		enhanced, aie := a.aiEnhanceMetadata(ctx, title, desc)
		if aie == nil && enhanced.Title != "" {
			title = sanitizeBiliTitle(enhanced.Title)
			if len(enhanced.Tags) > 0 {
				tags = sanitizeBiliTags(strings.Join(enhanced.Tags, ","))
			}
			if enhanced.Summary != "" {
				desc = sanitizeBiliDesc(enhanced.Summary)
			}
			if enhanced.Tid != "" && (q.Tid == "" || q.Tid == "188") {
				tid = enhanced.Tid
			}
		}
	}

	cover := q.Cover
	if cover == "" {
		cover = adjacentCover(files[0])
	}
	if q.Progress != nil {
		q.Progress("[ffmpeg] 正在优化投稿封面")
	}
	cover = a.sanitizeBiliCover(ctx, cover, q.Progress)

	// Multi-endpoint submission with smart auto-healing
	submitEndpoints := []string{"web", "b-cut-android", "app"}
	var totalLogs string
	var execErr error
	var bvid string

	attempt := 0
	useCover := (cover != "")

	for _, ep := range submitEndpoints {
		attempt++
		args := append([]string{"--user-cookie", a.cfg.BiliCookies, "upload"}, uploadFiles...)
		args = append(args, "--title", title, "--desc", desc)

		if useCover && cover != "" {
			args = append(args, "--cover", cover)
		}
		if tags != "" {
			args = append(args, "--tag", tags)
		}
		args = append(args, "--tid", tid)

		if q.Source != "" {
			args = append(args, "--copyright", "2", "--source", q.Source)
		} else {
			args = append(args, "--copyright", "1")
		}

		limit := q.Limit
		if limit == "" {
			// Multi-P uploads benefit from biliup's bounded parallelism. Keep
			// single-video uploads conservative, but use three workers for the
			// pipeline's chapter parts unless the caller explicitly overrides it.
			if q.Parts && len(uploadFiles) > 1 {
				limit = "3"
			} else {
				limit = "1"
			}
		}
		args = append(args, "--limit", limit, "--submit", ep, "--extra-fields", `{"open_subtitle":true}`)

		epLogs, err := runCmdProgress(ctx, a.cfg.Biliup, args, q.Progress)
		totalLogs += fmt.Sprintf("[%s 提交尝试 #%d]\n%s\n", ep, attempt, epLogs)

		res := parseBiliupOutput(epLogs)

		// 1. Success!
		if res.Code == 0 || res.BVID != "" {
			bvid = res.BVID
			execErr = nil
			break
		}

		// 2. Automated Code Analysis & Self-Healing Decision
		switch biliRepairActionFor(res.Code) {
		case biliRepairStop:
			if res.Code == 21070 || res.Code == 21071 {
				execErr = fmt.Errorf("B站提示：检测到重复稿件或相同视频正在审核中 (code %d: %s)", res.Code, res.Message)
			} else if res.Code == 21564 {
				execErr = fmt.Errorf("B站账号今日投稿频次已达限制 (code %d: %s)，已自动保护队列", res.Code, res.Message)
			} else {
				execErr = fmt.Errorf("B站登录凭证失效 (code %d: %s)，请在控制台更新 cookies.json", res.Code, res.Message)
			}
			goto finish

		case biliRepairRateLimit:
			// 601 is a server-side upload throttle. Switching endpoints or
			// immediately retrying only amplifies the throttle, so stop this
			// attempt and let the queue retry after an operator/chill-down period.
			totalLogs += fmt.Sprintf("[自动化自愈] B站上传限速 (code %d)，停止快速切换线路，保留本地文件等待冷却后重试。\n", res.Code)
			execErr = fmt.Errorf("B站上传限速 (code 601: %s)，请等待冷却后重试", res.Message)
			goto finish

		case biliRepairTitle:
			totalLogs += fmt.Sprintf("[自动化自愈] 捕获标题问题 (code %d: %s)，正在自动净化并精简标题...\n", res.Code, res.Message)
			title = sanitizeBiliTitle(strings.Map(func(r rune) rune {
				if r > 127 && r < 256 {
					return -1
				}
				return r
			}, title))
			if len([]rune(title)) > 40 {
				title = string([]rune(title)[:40])
			}
			continue

		case biliRepairDesc:
			totalLogs += fmt.Sprintf("[自动化自愈] 捕获简介问题 (code %d: %s)，正在自动清洗外链并精简简介...\n", res.Code, res.Message)
			desc = sanitizeBiliDesc(title)
			continue

		case biliRepairTags:
			totalLogs += fmt.Sprintf("[自动化自愈] 捕获标签不合规 (code %d: %s)，自动切换为安全通用标签...\n", res.Code, res.Message)
			tags = "科技,软件应用,教程"
			continue

		case biliRepairTID:
			totalLogs += fmt.Sprintf("[自动化自愈] 捕获分区 TID %s 无效 (code %d: %s)，自动回退至软件应用分区 (TID: 188)...\n", tid, res.Code, res.Message)
			tid = "188"
			continue

		case biliRepairCover:
			totalLogs += fmt.Sprintf("[自动化自愈] 捕获封面尺寸/格式不合规 (code %d: %s)，自动移除封面使用首帧...\n", res.Code, res.Message)
			useCover = false
			continue

		case biliRepairSwitch:
			totalLogs += fmt.Sprintf("[自动化自愈] 提交接口不兼容 (code %d: %s)，切换备用 Biliup 提交通道...\n", res.Code, res.Message)
			continue

		default:
			if err != nil {
				execErr = err
			} else {
				execErr = fmt.Errorf("B站投稿失败 (code %d: %s)", res.Code, res.Message)
			}
		}

		time.Sleep(2 * time.Second)
	}

finish:
	if bvid == "" {
		re := regexp.MustCompile(`BV[a-zA-Z0-9]{10}`)
		if m := re.FindString(totalLogs); m != "" {
			bvid = m
			execErr = nil
		}
	}

	biliURL := ""
	if bvid != "" {
		biliURL = "https://www.bilibili.com/video/" + bvid
	}

	res := map[string]any{
		"title":       title,
		"description": desc,
		"tags":        tags,
		"tid":         tid,
		"files":       files,
		"cover":       cover,
		"bvid":        bvid,
		"bili_url":    biliURL,
	}
	if translatedCount > 0 {
		totalLogs = partTranslateLogs + totalLogs
		res["translated_parts"] = translatedCount
	}
	return res, totalLogs, execErr
}

func (a *App) upload(w http.ResponseWriter, r *http.Request) {
	var q uploadReq
	if decode(r, &q) != nil || (q.File == "" && len(q.Files) == 0) {
		jsonResp(w, 400, map[string]string{"error": "file or files required"})
		return
	}
	j := a.add("biliup", q)
	a.dispatchJob(j)
	jsonResp(w, 202, j)
}

func (a *App) createUploadHandler(q uploadReq) func(*Job) {
	return func(nj *Job) {
		go a.runWithSlot(nj, a.uploadSlots, func() (any, string, error) {
			a.setStep(nj, "B站投稿中")
			a.setProgress(nj, JobProgress{Detail: "B站上传"})
			q.Progress = func(line string) { a.progressLine(nj, "B站上传", line) }
			out, logs, err := a.executeBiliupUpload(nj.ctx, q)
			if err == nil {
				// Direct uploads from the media library should have the same
				// post-success cleanup behavior as the auto pipeline. Keep
				// metadata (cover/subtitles/description) for audit and retry
				// context, but remove only the video files that were uploaded.
				files := q.Files
				if len(files) == 0 && q.File != "" {
					files = []string{q.File}
				}
				if freed := a.purgeManagedVideoFiles(files...); freed > 0 {
					if out == nil {
						out = map[string]any{}
					}
					freedStr := formatBytes(freed)
					logs += fmt.Sprintf("\n[自动空间清理] B站投稿成功，已自动删除原视频文件，释放磁盘空间: %s\n", freedStr)
					out["cleaned_disk"] = freedStr
				}
			}
			return out, logs, err
		})
	}
}

// One-Click End-to-End Pipeline
type pipelineReq struct {
	URL           string `json:"url"` // YouTube or Magnet
	SubLangs      string `json:"sub_langs"`
	Quality       string `json:"quality"`
	Translate     bool   `json:"translate"`
	Tid           string `json:"tid"`
	Tags          string `json:"tags"`
	SplitChapters bool   `json:"split_chapters"` // 段落自动分P
	BurnSubs      bool   `json:"burn_subs"`      // 显式为 true 才压制；默认复用 YouTube 字幕
}

func (a *App) pipeline(w http.ResponseWriter, r *http.Request) {
	var q pipelineReq
	if decode(r, &q) != nil || q.URL == "" {
		jsonResp(w, 400, map[string]string{"error": "target URL or Magnet required"})
		return
	}
	j := a.add("pipeline", q)
	a.dispatchJob(j)
	jsonResp(w, 202, j)
}

func (a *App) createPipelineHandler(q pipelineReq) func(*Job) {
	return func(nj *Job) {
		go func() {
			isYT := validYouTube(q.URL)
			isMag := strings.HasPrefix(q.URL, "magnet:") || validTorrentOrMagnet(q.URL)

			if !isYT && !isMag {
				a.set(nj, "failed", "unsupported URL (must be YouTube URL or Magnet URI)", nil, "")
				return
			}

			var targetDir string
			var totalLogs string
			var targetUploadFiles []string
			var mainVideoFile string
			var downloadErr error

			// Stage 1: Download stage (acquires downloadSlots)
			func() {
				if err := a.acquireSlot(nj.ctx, a.downloadSlots); err != nil {
					downloadErr = err
					return
				}
				defer func() {
					<-a.downloadSlots
					runtime.GC()
					debug.FreeOSMemory()
				}()

				if err := a.ensureSafeMemory(nj.ctx); err != nil {
					downloadErr = err
					return
				}

				a.set(nj, "running", "", nil, "")
				if isYT {
					a.setStep(nj, "[1/3] YouTube 视频解析与下载")
					d := filepath.Join(a.cfg.DataDir, "youtube", nj.ID)
					_ = os.MkdirAll(d, 0750)
					targetDir = d
					cookiePath, cleanup, _ := prepareCookies(a.cfg.Cookies, d)
					defer cleanup()

					langs := strings.TrimSpace(q.SubLangs)
					if langs == "" {
						langs = "zh-Hans,zh,en,zh-Hant"
					}
					isPlaylist := isPlaylistURL(q.URL)
					splitChapters, splitLog := a.chapterSplitDecision(nj.ctx, q.URL, cookiePath, q.SplitChapters)
					if splitLog != "" {
						totalLogs += splitLog + "\n"
					}
					args := []string{
						"--ignore-errors",
						"--no-abort-on-error",
						"--buffer-size", "16K",
						"--http-chunk-size", "10M",
						"--concurrent-fragments", "1",
						"--no-cache-dir",
						"--no-plugin-dirs",
						"--newline",
						"--progress-template", "download:%(progress._percent_str)s|%(progress.downloaded_bytes)s|%(progress.total_bytes)s|%(progress.speed)s|%(progress.eta)s|%(info.title)s",
						"--postprocessor-args", "ffmpeg:-threads 1",
						"--extractor-args", "youtube:player_client=android,ios,web,tv_downgraded,default",
					}
					if isPlaylist {
						args = append(args, "--yes-playlist")
					} else {
						args = append(args, "--no-playlist")
					}
					if cookiePath != "" {
						args = append(args, "--cookies", cookiePath)
					}
					if langs != "none" && langs != "no" {
						args = append(args, "--write-subs", "--sub-langs", langs, "--embed-subs")
					}

					switch q.Quality {
					case "audio_only":
						args = append(args, "-x", "--audio-format", "mp3")
					case "720p":
						args = append(args, "-f", "22/bv*[height<=720][ext=mp4]+ba[ext=m4a]/b[height<=720][ext=mp4]/bv*[height<=720]+ba/b/18")
					case "1080p":
						args = append(args, "-f", "bv*[height<=1080][ext=mp4]+ba[ext=m4a]/b[height<=1080][ext=mp4]/bv*[height<=1080]+ba/b/22/18")
					default:
						args = append(args, "-f", "bv*[ext=mp4]+ba[ext=m4a]/bv*+ba/b[ext=mp4]/b/22/18")
					}

					if isPlaylist {
						if splitChapters {
							args = append(args,
								"--split-chapters",
								"-o", "chapter:"+filepath.Join(d, "P%(playlist_index|1)02d - C%(section_number)02d. %(section_title)s.%(ext)s"),
							)
						}
						args = append(args,
							"--write-thumbnail",
							"--write-description",
							"--embed-metadata",
							"--merge-output-format", "mp4",
							"-o", filepath.Join(d, "P%(playlist_index|1)02d. %(title)s [%(id)s].%(ext)s"),
							q.URL,
						)
					} else {
						if splitChapters {
							args = append(args,
								"--split-chapters",
								"-o", "chapter:"+filepath.Join(d, "%(title)s - P%(section_number)02d. %(section_title)s.%(ext)s"),
							)
						}
						args = append(args,
							"--write-thumbnail",
							"--write-description",
							"--embed-metadata",
							"--merge-output-format", "mp4",
							"-o", filepath.Join(d, "%(title)s [%(id)s].%(ext)s"),
							q.URL,
						)
					}

					ytLogs, err := runCmdProgress(nj.ctx, a.cfg.YTDLP, args, func(line string) { a.progressLine(nj, "YouTube 下载", line) })
					totalLogs += "[YouTube Download Logs]\n" + ytLogs + "\n"
					downloadErr = err
					if downloadErr != nil {
						return
					}

					files, _ := filepath.Glob(filepath.Join(d, "*"))
					sort.Strings(files)
					var videoFiles []string
					var chapterFiles []string
					for _, f := range files {
						name := filepath.Base(f)
						ext := strings.ToLower(filepath.Ext(name))
						if ext == ".mp4" || ext == ".mkv" || ext == ".webm" || ext == ".mp3" {
							if strings.Contains(name, " - P") || strings.Contains(name, " - C") {
								chapterFiles = append(chapterFiles, f)
							} else {
								videoFiles = append(videoFiles, f)
							}
						}
					}
					targetUploadFiles = videoFiles
					if len(chapterFiles) > 0 {
						targetUploadFiles = chapterFiles
					}

					convertVttToSrtAndBcc(d)
					if q.BurnSubs && len(targetUploadFiles) > 0 {
						a.setStep(nj, "[1/3] 正在压制中英硬字幕...")
						a.setProgress(nj, JobProgress{Detail: "ffmpeg 字幕压制"})
						burned, bLogs, _ := burnSubtitlesToVideos(nj.ctx, d, targetUploadFiles, func(line string) { a.progressLine(nj, "ffmpeg 字幕压制", line) })
						targetUploadFiles = burned
						totalLogs += "\n[字幕压制日志]\n" + bLogs
					}
				} else {
					a.setStep(nj, "[1/3] 磁力高速抓取中")
					d := filepath.Join(a.cfg.DataDir, "magnet", nj.ID)
					_ = os.MkdirAll(d, 0750)
					targetDir = d
					btTrackers := "udp://tracker.opentrackr.org:1337/announce,udp://open.tracker.cl:1337/announce,udp://tracker.openbittorrent.com:6969/announce,http://tracker.openbittorrent.com:80/announce,udp://opentracker.i2p.rocks:6969/announce,udp://open.demonii.com:1337/announce"
					magLogs, err := a.runMagnetProgress(nj.ctx, []string{
						"--dir=" + d,
						"--continue=true",
						"--allow-overwrite=false",
						"--seed-time=0",
						"--file-allocation=none",
						"--disk-cache=16M",
						"--timeout=30",
						"--connect-timeout=15",
						"--bt-tracker-connect-timeout=15",
						"--bt-tracker-timeout=20",
						"--listen-port=" + a.cfg.BTListenPort,
						"--max-connection-per-server=4",
						"--bt-max-peers=60",
						"--max-concurrent-downloads=1",
						"--enable-dht=true",
						"--enable-peer-exchange=true",
						"--bt-enable-lpd=true",
						"--follow-torrent=mem",
						"--bt-stop-timeout=300",
						"--summary-interval=1",
						"--bt-tracker=" + btTrackers,
						q.URL,
					}, func(line string) { a.progressLine(nj, "BT 下载", line) })
					totalLogs += "[Magnet Download Logs]\n" + magLogs + "\n"
					downloadErr = err
					if downloadErr != nil {
						return
					}

					_ = filepath.Walk(d, func(p string, info os.FileInfo, err error) error {
						if err != nil || info.IsDir() {
							return nil
						}
						name := info.Name()
						ext := strings.ToLower(filepath.Ext(name))
						if ext == ".mp4" || ext == ".mkv" || ext == ".avi" || ext == ".webm" || ext == ".mp3" {
							targetUploadFiles = append(targetUploadFiles, p)
						}
						return nil
					})
					sort.Strings(targetUploadFiles)
				}
			}()

			if downloadErr != nil {
				if errors.Is(downloadErr, context.Canceled) {
					a.set(nj, "canceled", "已取消", map[string]any{"dir": targetDir}, totalLogs)
				} else {
					a.set(nj, "failed", downloadErr.Error(), map[string]any{"dir": targetDir}, totalLogs)
				}
				return
			}

			if len(targetUploadFiles) == 0 {
				a.set(nj, "failed", "no video files found after download", map[string]any{"dir": targetDir}, totalLogs)
				return
			}

			downBytes := calcFilesSize(targetUploadFiles)
			if downBytes == 0 {
				downBytes = calcDirSize(targetDir)
			}
			a.recordDownload(downBytes)

			mainVideoFile = targetUploadFiles[0]

			// Stage 2: Upload stage (acquires uploadSlots while downloadSlots is released!)
			var uploadOut map[string]any
			var uploadLogs string
			var uploadErr error

			func() {
				if err := a.acquireSlot(nj.ctx, a.uploadSlots); err != nil {
					uploadErr = err
					return
				}
				defer func() {
					<-a.uploadSlots
					runtime.GC()
					debug.FreeOSMemory()
				}()

				if err := a.ensureSafeMemory(nj.ctx); err != nil {
					uploadErr = err
					return
				}

				a.setStep(nj, "[2/3] AI 生成元数据与本地优化")
				a.setStep(nj, "[3/3] B站自动化并发投稿")
				a.setProgress(nj, JobProgress{Detail: "B站上传"})
				uploadOut, uploadLogs, uploadErr = a.executeBiliupUpload(nj.ctx, uploadReq{
					Files:     targetUploadFiles,
					File:      mainVideoFile,
					Translate: q.Translate,
					Tid:       q.Tid,
					Tag:       q.Tags,
					Parts:     true,
					Source:    q.URL,
					Progress:  func(line string) { a.progressLine(nj, "B站上传", line) },
				})
				totalLogs += "\n[Biliup Upload Logs]\n" + uploadLogs
			}()

			if uploadErr != nil {
				if errors.Is(uploadErr, context.Canceled) {
					a.set(nj, "canceled", "已取消", map[string]any{"dir": targetDir, "video_files": targetUploadFiles, "upload": uploadOut}, totalLogs)
				} else {
					a.set(nj, "failed", uploadErr.Error(), map[string]any{"dir": targetDir, "video_files": targetUploadFiles, "upload": uploadOut}, totalLogs)
				}
				return
			}

			upBytes := calcFilesSize(targetUploadFiles)
			if upBytes == 0 {
				upBytes = downBytes
			}
			a.recordUpload(upBytes)
			a.recordPipelineSuccess()
			if q.Translate {
				a.recordAiTrans()
			}

			// Auto-Clean downloaded raw video files after successful Bilibili upload
			freed := purgeVideoFiles(targetUploadFiles...)
			freedStr := ""
			if freed > 0 {
				freedStr = formatBytes(freed)
				totalLogs += fmt.Sprintf("\n[自动空间清理] B站投稿成功，已自动清除原视频文件，释放磁盘空间: %s\n", freedStr)
			}

			a.set(nj, "done", "", map[string]any{
				"dir":          targetDir,
				"video_file":   mainVideoFile,
				"video_files":  targetUploadFiles,
				"is_multi_p":   len(targetUploadFiles) > 1,
				"upload":       uploadOut,
				"cleaned_disk": freedStr,
			}, totalLogs)
		}()
	}
}

// AI Enhancement Model & Endpoint
type aiEnhanceResult struct {
	Title   string   `json:"title"`
	Summary string   `json:"summary"`
	Tags    []string `json:"tags"`
	Tid     string   `json:"tid"`
}

func freeTranslate(text, targetLang string) (string, error) {
	text = strings.TrimSpace(text)
	if text == "" {
		return "", nil
	}
	urlStr := fmt.Sprintf("https://translate.googleapis.com/translate_a/single?client=gtx&sl=auto&tl=%s&dt=t&q=%s",
		targetLang, url.QueryEscape(text))
	client := &http.Client{Timeout: 8 * time.Second}
	res, err := client.Get(urlStr)
	if err != nil {
		return "", err
	}
	defer res.Body.Close()

	if res.StatusCode != 200 {
		return "", fmt.Errorf("google translate http %d", res.StatusCode)
	}

	var raw []any
	if err := json.NewDecoder(res.Body).Decode(&raw); err != nil {
		return "", err
	}
	if len(raw) == 0 {
		return "", errors.New("empty response")
	}

	first, ok := raw[0].([]any)
	if !ok || len(first) == 0 {
		return "", errors.New("invalid format")
	}

	var sb strings.Builder
	for _, item := range first {
		if arr, ok := item.([]any); ok && len(arr) > 0 {
			if str, ok := arr[0].(string); ok {
				sb.WriteString(str)
			}
		}
	}
	return strings.TrimSpace(sb.String()), nil
}

func extractSmartKeywords(title, defaultTags string) []string {
	tagMap := make(map[string]bool)
	var tags []string

	// Add default tags
	for _, t := range strings.Split(defaultTags, ",") {
		t = strings.TrimSpace(t)
		if t != "" && !tagMap[t] {
			tagMap[t] = true
			tags = append(tags, t)
		}
	}

	// Extract title keywords (clean punctuation)
	clean := strings.Map(func(r rune) rune {
		if strings.ContainsRune("《》【】「」（）()[]-—_·,，.。/|", r) {
			return ' '
		}
		return r
	}, title)

	words := strings.Fields(clean)
	for _, w := range words {
		w = strings.TrimSpace(w)
		if len([]rune(w)) >= 2 && len([]rune(w)) <= 10 && !tagMap[w] && len(tags) < 6 {
			tagMap[w] = true
			tags = append(tags, w)
		}
	}
	return tags
}

func (a *App) callLLMEnhance(ctx context.Context, title, desc string) (*aiEnhanceResult, error) {
	if a.cfg.DeepSeekKey == "" {
		return nil, errors.New("LLM API key not configured")
	}

	prompt := fmt.Sprintf(`你是B站资深全能UP主助手。请根据提供的视频原标题与原英文背景信息，为该视频【全新生成】一套符合B站受众文化与推荐算法的高质量投稿元数据。

⚠️ 特别要求：
1. 标题（title）：不要机械直译！结合主题生成极具吸引力、自然地道且符合B站调性的爆款中文标题（50字内）。
2. 简介（summary）：【严禁直接复用或直译原外网简介！】请根据视频主题全新提炼视频看点、脉络与亮点总结（150-200字内），彻底过滤掉原视频中的社交媒体外链（Twitter/Instagram/Patreon）、赞助广告与商单购买链接，并带有自然的B站互动引导语。
3. 标签（tags）：输出5-8个精准高热度中文标签。
4. 分区（tid）：准确推荐分区ID（如 188 软件应用, 122 野生技术协会, 17 游戏, 28 音乐, 21 日常）。

请以严格的 JSON 格式输出：
{
  "title": "全新提炼的B站爆款中文标题",
  "summary": "全新生成的B站专属高质量中文简介（去噪点/提亮点）",
  "tags": ["标签1", "标签2", "标签3", "标签4", "标签5"],
  "tid": "188"
}

原标题：%s
原背景信息：%s`, title, desc)

	body, _ := json.Marshal(map[string]any{
		"model": a.cfg.DeepSeekModel,
		"messages": []map[string]string{
			{"role": "user", "content": prompt},
		},
		"temperature":     0.3,
		"response_format": map[string]string{"type": "json_object"},
	})

	req, _ := http.NewRequest("POST", a.cfg.DeepSeekURL, bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+a.cfg.DeepSeekKey)
	req.Header.Set("Content-Type", "application/json")
	req = req.WithContext(ctx)

	client := &http.Client{Timeout: 30 * time.Second}
	res, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.StatusCode/100 != 2 {
		return nil, fmt.Errorf("LLM HTTP %s", res.Status)
	}

	var v struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err = json.NewDecoder(io.LimitReader(res.Body, 1<<20)).Decode(&v); err != nil {
		return nil, err
	}
	if len(v.Choices) == 0 {
		return nil, errors.New("LLM returned empty response")
	}

	raw := strings.TrimSpace(v.Choices[0].Message.Content)
	raw = strings.TrimPrefix(raw, "```json")
	raw = strings.TrimPrefix(raw, "```")
	raw = strings.TrimSuffix(raw, "```")
	raw = strings.TrimSpace(raw)

	var rawResult struct {
		Title   string   `json:"title"`
		Summary string   `json:"summary"`
		Tags    []string `json:"tags"`
		Tid     any      `json:"tid"`
	}
	if err := json.Unmarshal([]byte(raw), &rawResult); err != nil {
		return nil, err
	}
	tidStr := "188"
	if rawResult.Tid != nil {
		tidStr = fmt.Sprintf("%v", rawResult.Tid)
	}
	return &aiEnhanceResult{
		Title:   strings.TrimSpace(rawResult.Title),
		Summary: strings.TrimSpace(rawResult.Summary),
		Tags:    rawResult.Tags,
		Tid:     tidStr,
	}, nil
}

func (a *App) aiEnhanceMetadata(ctx context.Context, title, desc string) (*aiEnhanceResult, error) {
	// 1. Try LLM if configured
	if a.cfg.DeepSeekKey != "" {
		res, err := a.callLLMEnhance(ctx, title, desc)
		if err == nil && res != nil && res.Title != "" {
			return res, nil
		}
	}

	// 2. Free Fallback: Online Google Translate & Smart Tag Extraction (0 cost, 0 key needed)
	zhTitle, err := freeTranslate(title, "zh-CN")
	if err != nil || zhTitle == "" {
		zhTitle = title
	}

	zhDesc := desc
	if desc != "" {
		if d, err := freeTranslate(trim200(desc), "zh-CN"); err == nil && d != "" {
			zhDesc = d
		}
	}

	return &aiEnhanceResult{
		Title:   zhTitle,
		Summary: zhDesc,
		Tags:    extractSmartKeywords(zhTitle, a.cfg.DefaultTags),
		Tid:     "188",
	}, nil
}

func (a *App) aiEnhanceHandler(w http.ResponseWriter, r *http.Request) {
	var q struct {
		Title       string `json:"title"`
		Description string `json:"description"`
	}
	if decode(r, &q) != nil || (q.Title == "" && q.Description == "") {
		jsonResp(w, 400, map[string]string{"error": "title or description required"})
		return
	}
	res, err := a.aiEnhanceMetadata(r.Context(), q.Title, q.Description)
	if err != nil {
		jsonResp(w, 500, map[string]string{"error": err.Error()})
		return
	}
	jsonResp(w, 200, res)
}

// Media Package & Clean File Manager
type MediaPackage struct {
	ID           string    `json:"id"`
	Source       string    `json:"source"` // "youtube", "magnet", "other"
	Title        string    `json:"title"`
	Folder       string    `json:"folder"`
	RelFolder    string    `json:"rel_folder"`
	VideoFile    string    `json:"video_file"` // Rel path to /files/...
	VideoName    string    `json:"video_name"`
	VideoCount   int       `json:"video_count"`
	VideoFiles   []string  `json:"video_files"`
	VideoSize    int64     `json:"video_size"`
	VideoSizeStr string    `json:"video_size_str"`
	CoverFile    string    `json:"cover_file"` // Rel path to /files/...
	Subtitles    []string  `json:"subtitles"`
	Description  string    `json:"description"`
	HasPart      bool      `json:"has_part"`
	TotalSize    int64     `json:"total_size"`
	TotalSizeStr string    `json:"total_size_str"`
	ModTime      time.Time `json:"mod_time"`
	Status       string    `json:"status"` // "ready", "incomplete", "empty"
}

func formatBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}

func formatSpeed(bps float64) string {
	if bps < 1024 {
		return fmt.Sprintf("%.0f B/s", bps)
	}
	if bps < 1024*1024 {
		return fmt.Sprintf("%.1f KB/s", bps/1024)
	}
	if bps < 1024*1024*1024 {
		return fmt.Sprintf("%.1f MB/s", bps/(1024*1024))
	}
	return fmt.Sprintf("%.2f GB/s", bps/(1024*1024*1024))
}

func calcFilesSize(files []string) int64 {
	var total int64
	for _, f := range files {
		if fi, err := os.Stat(f); err == nil && !fi.IsDir() {
			total += fi.Size()
		}
	}
	return total
}

func calcDirSize(dir string) int64 {
	var total int64
	_ = filepath.Walk(dir, func(p string, info os.FileInfo, err error) error {
		if err == nil && info != nil && !info.IsDir() {
			total += info.Size()
		}
		return nil
	})
	return total
}

func (a *App) scanMediaPackages() []MediaPackage {
	root := a.cfg.DataDir
	sources := []string{"youtube", "magnet"}
	pkgs := make([]MediaPackage, 0)

	for _, src := range sources {
		srcDir := filepath.Join(root, src)
		entries, err := os.ReadDir(srcDir)
		if err != nil {
			continue
		}
		for _, e := range entries {
			if !e.IsDir() {
				continue
			}
			folderID := e.Name()
			folderPath := filepath.Join(srcDir, folderID)
			relFolder := filepath.Join(src, folderID)

			var pkg MediaPackage
			pkg.ID = folderID
			pkg.Source = src
			pkg.Folder = folderPath
			pkg.RelFolder = relFolder
			pkg.Status = "empty"

			var latestMod time.Time
			var totalSize int64
			var videoPath string
			var coverPath string
			var descText string

			_ = filepath.Walk(folderPath, func(p string, info os.FileInfo, err error) error {
				if err != nil || info.IsDir() {
					return nil
				}
				name := info.Name()
				totalSize += info.Size()
				if info.ModTime().After(latestMod) {
					latestMod = info.ModTime()
				}

				ext := strings.ToLower(filepath.Ext(name))
				if strings.HasSuffix(name, ".part") || strings.HasSuffix(name, ".ytdl") || strings.HasSuffix(name, ".aria2") {
					pkg.HasPart = true
				}

				relFile, _ := filepath.Rel(root, p)
				if ext == ".mp4" || ext == ".mkv" || ext == ".avi" || ext == ".mp3" || ext == ".webm" {
					pkg.VideoFiles = append(pkg.VideoFiles, p)
					pkg.VideoCount++
					if videoPath == "" || ext == ".mp4" {
						videoPath = name
						pkg.VideoName = name
						pkg.VideoFile = relFile
						pkg.VideoSize = info.Size()
						pkg.VideoSizeStr = formatBytes(info.Size())
						pkg.Status = "ready"
					}
				} else if ext == ".vtt" || ext == ".srt" {
					pkg.Subtitles = append(pkg.Subtitles, name)
				} else if ext == ".jpg" || ext == ".jpeg" || ext == ".png" || ext == ".webp" {
					if coverPath == "" || ext == ".jpg" || ext == ".png" {
						coverPath = name
						pkg.CoverFile = relFile
					}
				} else if ext == ".description" {
					b, _ := os.ReadFile(p)
					descText = string(b)
				}
				return nil
			})

			if pkg.Status != "ready" && pkg.HasPart {
				pkg.Status = "incomplete"
			}

			pkg.TotalSize = totalSize
			pkg.TotalSizeStr = formatBytes(totalSize)
			pkg.ModTime = latestMod

			if pkg.VideoName != "" {
				pkg.Title = strings.TrimSuffix(pkg.VideoName, filepath.Ext(pkg.VideoName))
			} else if descText != "" {
				lines := strings.Split(descText, "\n")
				if len(lines) > 0 {
					pkg.Title = lines[0]
				}
			}
			if pkg.Title == "" {
				pkg.Title = folderID
			}
			if len(descText) > 200 {
				pkg.Description = descText[:200] + "…"
			} else {
				pkg.Description = descText
			}

			pkgs = append(pkgs, pkg)
		}
	}

	return pkgs
}

func (a *App) listMediaHandler(w http.ResponseWriter, r *http.Request) {
	pkgs := a.scanMediaPackages()
	jsonResp(w, 200, pkgs)
}

func (a *App) cleanTempHandler(w http.ResponseWriter, r *http.Request) {
	root := a.cfg.DataDir
	deletedCount := 0
	var freedBytes int64

	// Delete .part, .ytdl, .aria2 files
	_ = filepath.Walk(root, func(p string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		name := info.Name()
		if strings.HasSuffix(name, ".part") || strings.HasSuffix(name, ".ytdl") || strings.HasSuffix(name, ".aria2") {
			freedBytes += info.Size()
			_ = os.Remove(p)
			deletedCount++
		}
		return nil
	})

	// Clean empty or abandoned directories without video/media files
	for _, src := range []string{"youtube", "magnet"} {
		srcDir := filepath.Join(root, src)
		entries, err := os.ReadDir(srcDir)
		if err != nil {
			continue
		}
		for _, e := range entries {
			if !e.IsDir() {
				continue
			}
			subPath := filepath.Join(srcDir, e.Name())
			hasMedia := false
			_ = filepath.Walk(subPath, func(p string, info os.FileInfo, err error) error {
				if err != nil || info.IsDir() {
					return nil
				}
				ext := strings.ToLower(filepath.Ext(info.Name()))
				if ext == ".mp4" || ext == ".mkv" || ext == ".avi" || ext == ".mp3" || ext == ".webm" {
					hasMedia = true
				}
				return nil
			})
			if !hasMedia {
				_ = os.RemoveAll(subPath)
				deletedCount++
			}
		}
	}

	jsonResp(w, 200, map[string]any{
		"ok":            true,
		"deleted_count": deletedCount,
		"freed_bytes":   freedBytes,
		"freed_str":     formatBytes(freedBytes),
	})
}

func (a *App) deleteMediaHandler(w http.ResponseWriter, r *http.Request) {
	var q struct {
		Folder string `json:"folder"`
	}
	if decode(r, &q) != nil || q.Folder == "" {
		jsonResp(w, 400, map[string]string{"error": "folder path required"})
		return
	}
	clean := filepath.Clean(q.Folder)
	if !strings.HasPrefix(clean, a.cfg.DataDir) {
		jsonResp(w, 403, map[string]string{"error": "forbidden path outside data dir"})
		return
	}
	if err := os.RemoveAll(clean); err != nil {
		jsonResp(w, 500, map[string]string{"error": err.Error()})
		return
	}
	jsonResp(w, 200, map[string]any{"ok": true, "folder": clean})
}

// ==========================================
// YouTube Channel Monitor & Auto-Sync Engine
// ==========================================
type ytPlaylistEntry struct {
	ID    string `json:"id"`
	Title string `json:"title"`
	URL   string `json:"url"`
}

type ytPlaylistDump struct {
	Channel  string            `json:"channel"`
	Title    string            `json:"title"`
	Uploader string            `json:"uploader"`
	Entries  []ytPlaylistEntry `json:"entries"`
}

func (a *App) channelsFilePath() string {
	if a.cfg.ChannelsFile != "" {
		return a.cfg.ChannelsFile
	}
	return filepath.Join(a.cfg.DataDir, "channels.json")
}

func (a *App) loadChannels() {
	a.cmu.Lock()
	defer a.cmu.Unlock()
	a.channels = make(map[string]*MonitoredChannel)
	a.channelOrder = nil

	data, err := os.ReadFile(a.channelsFilePath())
	if err != nil {
		return
	}
	var list []*MonitoredChannel
	if err := json.Unmarshal(data, &list); err != nil {
		return
	}
	for _, ch := range list {
		if ch == nil || ch.ID == "" {
			continue
		}
		if ch.SyncedIDs == nil {
			ch.SyncedIDs = make(map[string]bool)
		}
		a.channels[ch.ID] = ch
		a.channelOrder = append(a.channelOrder, ch.ID)
	}
}

func (a *App) saveChannelsLocked() {
	list := make([]*MonitoredChannel, 0, len(a.channelOrder))
	for _, id := range a.channelOrder {
		if ch := a.channels[id]; ch != nil {
			list = append(list, ch)
		}
	}

	data, err := json.MarshalIndent(list, "", "  ")
	if err != nil {
		return
	}
	filePath := a.channelsFilePath()
	tmp := filePath + ".tmp"
	if err := os.WriteFile(tmp, data, 0640); err == nil {
		_ = os.Rename(tmp, filePath)
	}
}

func (a *App) saveChannels() {
	a.cmu.Lock()
	defer a.cmu.Unlock()
	a.saveChannelsLocked()
}

func (a *App) listChannels() []*MonitoredChannel {
	a.cmu.RLock()
	defer a.cmu.RUnlock()
	out := make([]*MonitoredChannel, 0, len(a.channelOrder))
	for _, id := range a.channelOrder {
		if ch := a.channels[id]; ch != nil {
			out = append(out, ch)
		}
	}
	return out
}

func normalizeChannelURL(rawURL string) string {
	rawURL = strings.TrimSpace(rawURL)
	if strings.Contains(rawURL, "/playlist") || strings.Contains(rawURL, "list=") || strings.HasSuffix(rawURL, "/videos") || strings.HasSuffix(rawURL, "/shorts") || strings.HasSuffix(rawURL, "/streams") {
		return rawURL
	}
	trimmed := strings.TrimRight(rawURL, "/")
	if strings.Contains(trimmed, "@") || strings.Contains(trimmed, "/channel/") || strings.Contains(trimmed, "/c/") || strings.Contains(trimmed, "/user/") {
		return trimmed + "/videos"
	}
	return rawURL
}

func (a *App) addChannel(ch MonitoredChannel) (*MonitoredChannel, error) {
	ch.URL = strings.TrimSpace(ch.URL)
	if !validYouTube(ch.URL) && !strings.Contains(ch.URL, "youtube.com") {
		return nil, errors.New("invalid YouTube Channel or Playlist URL")
	}
	a.cmu.Lock()
	defer a.cmu.Unlock()

	for _, existing := range a.channels {
		if existing.URL == ch.URL {
			return existing, nil
		}
	}

	newCh := &MonitoredChannel{
		ID:                   id(),
		URL:                  ch.URL,
		Title:                ch.Title,
		Uploader:             ch.Uploader,
		Enabled:              true,
		CheckIntervalMinutes: ch.CheckIntervalMinutes,
		Translate:            ch.Translate,
		Tid:                  ch.Tid,
		Tags:                 ch.Tags,
		Quality:              ch.Quality,
		SplitChapters:        ch.SplitChapters,
		MaxPerCheck:          ch.MaxPerCheck,
		SyncedIDs:            make(map[string]bool),
		CreatedAt:            time.Now(),
	}
	if newCh.CheckIntervalMinutes < 10 {
		newCh.CheckIntervalMinutes = 60
	}
	if newCh.MaxPerCheck <= 0 {
		newCh.MaxPerCheck = 2
	}
	if newCh.Tid == "" {
		newCh.Tid = "188"
	}
	if newCh.Tags == "" {
		newCh.Tags = a.cfg.DefaultTags
	}
	if newCh.Quality == "" {
		newCh.Quality = "1080p"
	}

	a.channels[newCh.ID] = newCh
	a.channelOrder = append(a.channelOrder, newCh.ID)
	a.saveChannelsLocked()
	return newCh, nil
}

func (a *App) toggleChannel(id string) (*MonitoredChannel, error) {
	a.cmu.Lock()
	defer a.cmu.Unlock()
	ch := a.channels[id]
	if ch == nil {
		return nil, errors.New("channel not found")
	}
	ch.Enabled = !ch.Enabled
	a.saveChannelsLocked()
	return ch, nil
}

func (a *App) deleteChannel(id string) error {
	a.cmu.Lock()
	defer a.cmu.Unlock()
	if a.channels[id] == nil {
		return errors.New("channel not found")
	}
	delete(a.channels, id)
	newOrder := make([]string, 0, len(a.channelOrder))
	for _, oid := range a.channelOrder {
		if oid != id {
			newOrder = append(newOrder, oid)
		}
	}
	a.channelOrder = newOrder
	a.saveChannelsLocked()
	return nil
}

func (a *App) syncChannel(ctx context.Context, ch *MonitoredChannel) (int, error) {
	if ch == nil || ch.URL == "" {
		return 0, errors.New("invalid channel")
	}

	maxFetch := ch.MaxPerCheck
	if maxFetch <= 0 {
		maxFetch = 2
	}
	if maxFetch > 10 {
		maxFetch = 10
	}

	probeURL := normalizeChannelURL(ch.URL)
	args := []string{
		"--flat-playlist",
		"--dump-single-json",
		"--no-warnings",
		"--no-plugin-dirs",
		"--playlist-end", strconv.Itoa(maxFetch),
		probeURL,
	}

	out, err := runCmd(ctx, a.cfg.YTDLP, args)
	a.cmu.Lock()
	ch.LastCheckedAt = time.Now()
	if err != nil {
		a.saveChannelsLocked()
		a.cmu.Unlock()
		return 0, fmt.Errorf("fetch channel failed: %w", err)
	}

	var dump ytPlaylistDump
	if err := json.Unmarshal([]byte(out), &dump); err != nil {
		a.saveChannelsLocked()
		a.cmu.Unlock()
		return 0, fmt.Errorf("parse channel json failed: %w", err)
	}

	if dump.Channel != "" {
		ch.Title = dump.Channel
	} else if dump.Title != "" && ch.Title == "" {
		ch.Title = dump.Title
	}
	if dump.Uploader != "" {
		ch.Uploader = dump.Uploader
	}

	if ch.SyncedIDs == nil {
		ch.SyncedIDs = make(map[string]bool)
	}

	newCount := 0
	for i := len(dump.Entries) - 1; i >= 0; i-- {
		entry := dump.Entries[i]
		vid := entry.ID
		if vid == "" {
			continue
		}
		if ch.SyncedIDs[vid] {
			continue
		}

		targetURL := entry.URL
		if targetURL == "" || !strings.HasPrefix(targetURL, "http") {
			targetURL = "https://www.youtube.com/watch?v=" + vid
		}

		req := pipelineReq{
			URL:           targetURL,
			Translate:     ch.Translate,
			Tid:           ch.Tid,
			Tags:          ch.Tags,
			Quality:       ch.Quality,
			SplitChapters: ch.SplitChapters,
		}
		j := a.add("pipeline", req)
		a.dispatchJob(j)

		ch.SyncedIDs[vid] = true
		ch.LastSyncedAt = time.Now()
		ch.LastSyncedTitle = entry.Title
		ch.LastSyncedVideoID = vid
		ch.SyncCount++
		newCount++
	}

	a.saveChannelsLocked()
	a.cmu.Unlock()
	return newCount, nil
}

func (a *App) startChannelWatcher(ctx context.Context) {
	ticker := time.NewTicker(60 * time.Second)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				a.checkAllChannels(ctx)
			}
		}
	}()
}

func (a *App) checkAllChannels(ctx context.Context) {
	a.cmu.RLock()
	var toCheck []*MonitoredChannel
	now := time.Now()
	for _, id := range a.channelOrder {
		ch := a.channels[id]
		if ch == nil || !ch.Enabled {
			continue
		}
		interval := ch.CheckIntervalMinutes
		if interval < 10 {
			interval = 60
		}
		if ch.LastCheckedAt.IsZero() || now.Sub(ch.LastCheckedAt) >= time.Duration(interval)*time.Minute {
			toCheck = append(toCheck, ch)
		}
	}
	a.cmu.RUnlock()

	for _, ch := range toCheck {
		if ctx.Err() != nil {
			break
		}
		_, _ = a.syncChannel(ctx, ch)
		time.Sleep(3 * time.Second)
	}
}

func (a *App) getChannelsHandler(w http.ResponseWriter, r *http.Request) {
	jsonResp(w, 200, a.listChannels())
}

func (a *App) createChannelHandler(w http.ResponseWriter, r *http.Request) {
	var ch MonitoredChannel
	if decode(r, &ch) != nil || ch.URL == "" {
		jsonResp(w, 400, map[string]string{"error": "valid YouTube channel url required"})
		return
	}
	added, err := a.addChannel(ch)
	if err != nil {
		jsonResp(w, 400, map[string]string{"error": err.Error()})
		return
	}
	go func(c *MonitoredChannel) {
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()
		_, _ = a.syncChannel(ctx, c)
	}(added)

	jsonResp(w, 201, added)
}

func (a *App) toggleChannelHandler(w http.ResponseWriter, r *http.Request, id string) {
	ch, err := a.toggleChannel(id)
	if err != nil {
		jsonResp(w, 404, map[string]string{"error": err.Error()})
		return
	}
	jsonResp(w, 200, ch)
}

func (a *App) syncChannelHandler(w http.ResponseWriter, r *http.Request, id string) {
	a.cmu.RLock()
	ch := a.channels[id]
	a.cmu.RUnlock()
	if ch == nil {
		jsonResp(w, 404, map[string]string{"error": "channel not found"})
		return
	}
	count, err := a.syncChannel(r.Context(), ch)
	if err != nil {
		jsonResp(w, 500, map[string]any{"error": err.Error(), "channel": ch})
		return
	}
	jsonResp(w, 200, map[string]any{"ok": true, "synced_new": count, "channel": ch})
}

func (a *App) deleteChannelHandler(w http.ResponseWriter, r *http.Request, id string) {
	if err := a.deleteChannel(id); err != nil {
		jsonResp(w, 404, map[string]string{"error": err.Error()})
		return
	}
	jsonResp(w, 200, map[string]any{"ok": true, "id": id})
}

// Hardware & System Diagnostics (ROM, RAM, CPU, Load)
type MemInfo struct {
	TotalMB     int     `json:"total_mb"`
	UsedMB      int     `json:"used_mb"`
	FreeMB      int     `json:"free_mb"`
	AvailableMB int     `json:"available_mb"`
	Percent     float64 `json:"percent"`
	Text        string  `json:"text"`
	SwapTotalMB int     `json:"swap_total_mb"`
	SwapUsedMB  int     `json:"swap_used_mb"`
	SwapPercent float64 `json:"swap_percent"`
}

func getMemoryInfo() MemInfo {
	b, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return MemInfo{}
	}
	var total, free, available, buffers, cached, swapTotal, swapFree int
	scanner := bufio.NewScanner(bytes.NewReader(b))
	for scanner.Scan() {
		line := scanner.Text()
		var val int
		if strings.HasPrefix(line, "MemTotal:") {
			_, _ = fmt.Sscanf(line, "MemTotal: %d kB", &val)
			total = val / 1024
		} else if strings.HasPrefix(line, "MemFree:") {
			_, _ = fmt.Sscanf(line, "MemFree: %d kB", &val)
			free = val / 1024
		} else if strings.HasPrefix(line, "MemAvailable:") {
			_, _ = fmt.Sscanf(line, "MemAvailable: %d kB", &val)
			available = val / 1024
		} else if strings.HasPrefix(line, "Buffers:") {
			_, _ = fmt.Sscanf(line, "Buffers: %d kB", &val)
			buffers = val / 1024
		} else if strings.HasPrefix(line, "Cached:") {
			_, _ = fmt.Sscanf(line, "Cached: %d kB", &val)
			cached = val / 1024
		} else if strings.HasPrefix(line, "SwapTotal:") {
			_, _ = fmt.Sscanf(line, "SwapTotal: %d kB", &val)
			swapTotal = val / 1024
		} else if strings.HasPrefix(line, "SwapFree:") {
			_, _ = fmt.Sscanf(line, "SwapFree: %d kB", &val)
			swapFree = val / 1024
		}
	}
	if available == 0 {
		available = free + buffers + cached
	}
	used := total - available
	if used < 0 {
		used = 0
	}
	percent := 0.0
	if total > 0 {
		percent = (float64(used) / float64(total)) * 100.0
	}
	swapUsed := swapTotal - swapFree
	swapPercent := 0.0
	if swapTotal > 0 {
		swapPercent = (float64(swapUsed) / float64(swapTotal)) * 100.0
	}
	return MemInfo{
		TotalMB:     total,
		UsedMB:      used,
		FreeMB:      total - used,
		AvailableMB: available,
		Percent:     math.Round(percent*10) / 10,
		Text:        fmt.Sprintf("%d MB / %d MB", used, total),
		SwapTotalMB: swapTotal,
		SwapUsedMB:  swapUsed,
		SwapPercent: math.Round(swapPercent*10) / 10,
	}
}

type CpuInfo struct {
	Load1  float64 `json:"load_1m"`
	Load5  float64 `json:"load_5m"`
	Load15 float64 `json:"load_15m"`
	Text   string  `json:"text"`
}

func getCpuLoad() CpuInfo {
	b, err := os.ReadFile("/proc/loadavg")
	if err != nil {
		return CpuInfo{}
	}
	var l1, l5, l15 float64
	_, _ = fmt.Sscanf(string(b), "%f %f %f", &l1, &l5, &l15)
	return CpuInfo{
		Load1:  l1,
		Load5:  l5,
		Load15: l15,
		Text:   fmt.Sprintf("%.2f, %.2f, %.2f", l1, l5, l15),
	}
}

type DiskInfo struct {
	TotalGB float64 `json:"total_gb"`
	UsedGB  float64 `json:"used_gb"`
	FreeGB  float64 `json:"free_gb"`
	Percent float64 `json:"percent"`
	Text    string  `json:"text"`
}

func getDiskInfo(dir string) DiskInfo {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(dir, &stat); err != nil {
		return DiskInfo{}
	}
	totalBytes := stat.Blocks * uint64(stat.Bsize)
	freeBytes := stat.Bavail * uint64(stat.Bsize)
	usedBytes := totalBytes - freeBytes
	totalGB := float64(totalBytes) / (1024 * 1024 * 1024)
	freeGB := float64(freeBytes) / (1024 * 1024 * 1024)
	usedGB := float64(usedBytes) / (1024 * 1024 * 1024)
	percent := 0.0
	if totalBytes > 0 {
		percent = (float64(usedBytes) / float64(totalBytes)) * 100.0
	}
	return DiskInfo{
		TotalGB: math.Round(totalGB*10) / 10,
		UsedGB:  math.Round(usedGB*10) / 10,
		FreeGB:  math.Round(freeGB*10) / 10,
		Percent: math.Round(percent*10) / 10,
		Text:    fmt.Sprintf("%.1f GB / %.1f GB", usedGB, totalGB),
	}
}

type NetworkStats struct {
	mu           sync.RWMutex
	lastTime     time.Time
	lastRxBytes  uint64
	lastTxBytes  uint64
	currRxSpeed  float64 // B/s
	currTxSpeed  float64 // B/s
	totalRxBytes uint64
	totalTxBytes uint64
}

func (n *NetworkStats) Sample() {
	rx, tx, err := readNetDevBytes()
	if err != nil {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()

	now := time.Now()
	if !n.lastTime.IsZero() {
		dt := now.Sub(n.lastTime).Seconds()
		if dt >= 0.5 {
			if rx >= n.lastRxBytes {
				n.currRxSpeed = float64(rx-n.lastRxBytes) / dt
			}
			if tx >= n.lastTxBytes {
				n.currTxSpeed = float64(tx-n.lastTxBytes) / dt
			}
			n.lastTime = now
			n.lastRxBytes = rx
			n.lastTxBytes = tx
		}
	} else {
		n.lastTime = now
		n.lastRxBytes = rx
		n.lastTxBytes = tx
	}
	n.totalRxBytes = rx
	n.totalTxBytes = tx
}

func readNetDevBytes() (uint64, uint64, error) {
	b, err := os.ReadFile("/proc/net/dev")
	if err != nil {
		return 0, 0, err
	}
	var totalRx, totalTx uint64
	scanner := bufio.NewScanner(bytes.NewReader(b))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if !strings.Contains(line, ":") {
			continue
		}
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue
		}
		iface := strings.TrimSpace(parts[0])
		if iface == "lo" {
			continue
		}
		fields := strings.Fields(parts[1])
		if len(fields) >= 9 {
			rx, _ := strconv.ParseUint(fields[0], 10, 64)
			tx, _ := strconv.ParseUint(fields[8], 10, 64)
			totalRx += rx
			totalTx += tx
		}
	}
	return totalRx, totalTx, nil
}

type AppStats struct {
	TotalDownloadedBytes int64  `json:"total_downloaded_bytes"` // 历史累计下载媒体数据量 (字节)
	TotalUploadedBytes   int64  `json:"total_uploaded_bytes"`   // 历史累计上传B站数据量 (字节)
	TotalDownloadsCount  int64  `json:"total_downloads_count"`  // 历史累计成功下载任务数
	TotalUploadsCount    int64  `json:"total_uploads_count"`    // 历史累计成功投稿数
	TotalPipelineCount   int64  `json:"total_pipeline_count"`   // 历史流水线执行数
	TotalAiTransCount    int64  `json:"total_ai_trans_count"`   // 历史AI处理数
	LastUpdated          string `json:"last_updated"`
}

func (a *App) statsFile() string {
	return filepath.Join(a.cfg.DataDir, "stats.json")
}

func (a *App) loadStats() {
	a.smu.Lock()
	defer a.smu.Unlock()

	b, err := os.ReadFile(a.statsFile())
	if err == nil {
		if json.Unmarshal(b, &a.stats) == nil {
			return
		}
	}

	// Bootstrap from existing files in DataDir
	pkgs := a.scanMediaPackages()
	var initDownBytes int64
	var downCount int64
	for _, p := range pkgs {
		if p.TotalSize > 0 {
			initDownBytes += p.TotalSize
			downCount++
		}
	}
	a.stats = AppStats{
		TotalDownloadedBytes: initDownBytes,
		TotalDownloadsCount:  downCount,
		TotalUploadedBytes:   0,
		TotalUploadsCount:    0,
		TotalPipelineCount:   0,
		TotalAiTransCount:    0,
		LastUpdated:          time.Now().Format(time.RFC3339),
	}
	a.saveStatsLocked()
}

func (a *App) saveStatsLocked() {
	a.stats.LastUpdated = time.Now().Format(time.RFC3339)
	b, err := json.MarshalIndent(a.stats, "", "  ")
	if err == nil {
		_ = os.WriteFile(a.statsFile(), b, 0640)
	}
}

func (a *App) recordDownload(bytes int64) {
	if bytes <= 0 {
		return
	}
	a.smu.Lock()
	defer a.smu.Unlock()
	a.stats.TotalDownloadedBytes += bytes
	a.stats.TotalDownloadsCount++
	a.saveStatsLocked()
}

func (a *App) recordUpload(bytes int64) {
	if bytes <= 0 {
		return
	}
	a.smu.Lock()
	defer a.smu.Unlock()
	a.stats.TotalUploadedBytes += bytes
	a.stats.TotalUploadsCount++
	a.saveStatsLocked()
}

func (a *App) recordPipelineSuccess() {
	a.smu.Lock()
	defer a.smu.Unlock()
	a.stats.TotalPipelineCount++
	a.saveStatsLocked()
}

func (a *App) recordAiTrans() {
	a.smu.Lock()
	defer a.smu.Unlock()
	a.stats.TotalAiTransCount++
	a.saveStatsLocked()
}

func (a *App) startNetworkSampler(ctx context.Context) {
	a.netStats.Sample()
	ticker := time.NewTicker(2 * time.Second)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				a.netStats.Sample()
			}
		}
	}()
}

func (a *App) systemDiagnostics() map[string]any {
	ram := getMemoryInfo()
	rom := getDiskInfo(a.cfg.DataDir)
	cpu := getCpuLoad()

	a.netStats.Sample()
	a.netStats.mu.RLock()
	netInfo := map[string]any{
		"rx_speed_bps":   a.netStats.currRxSpeed,
		"tx_speed_bps":   a.netStats.currTxSpeed,
		"rx_speed_text":  formatSpeed(a.netStats.currRxSpeed),
		"tx_speed_text":  formatSpeed(a.netStats.currTxSpeed),
		"rx_total_bytes": a.netStats.totalRxBytes,
		"tx_total_bytes": a.netStats.totalTxBytes,
		"rx_total_text":  formatBytes(int64(a.netStats.totalRxBytes)),
		"tx_total_text":  formatBytes(int64(a.netStats.totalTxBytes)),
	}
	a.netStats.mu.RUnlock()

	a.smu.RLock()
	curStats := a.stats
	a.smu.RUnlock()

	// Calculate media library current total
	pkgs := a.scanMediaPackages()
	var mediaLibBytes int64
	for _, p := range pkgs {
		mediaLibBytes += p.TotalSize
	}

	trafficStats := map[string]any{
		"total_downloaded_bytes": curStats.TotalDownloadedBytes,
		"total_downloaded_text":  formatBytes(curStats.TotalDownloadedBytes),
		"total_uploaded_bytes":   curStats.TotalUploadedBytes,
		"total_uploaded_text":    formatBytes(curStats.TotalUploadedBytes),
		"total_downloads_count":  curStats.TotalDownloadsCount,
		"total_uploads_count":    curStats.TotalUploadsCount,
		"total_pipeline_count":   curStats.TotalPipelineCount,
		"total_ai_trans_count":   curStats.TotalAiTransCount,
		"media_library_bytes":    mediaLibBytes,
		"media_library_text":     formatBytes(mediaLibBytes),
		"media_packages_count":   len(pkgs),
		"last_updated":           curStats.LastUpdated,
	}

	a.mu.RLock()
	totalJobs := len(a.order)
	runningJobs := 0
	for _, j := range a.jobs {
		if j != nil && j.Status == "running" {
			runningJobs++
		}
	}
	a.mu.RUnlock()

	// Check Bilibili cookie mid
	biliMid := "未登录"
	biliExpires := "未知"
	b, err := os.ReadFile(a.cfg.BiliCookies)
	if err == nil {
		var env cookieEnvelope
		_ = json.Unmarshal(b, &env)
		for _, c := range env.CookieInfo.Cookies {
			if c.Name == "DedeUserID" {
				biliMid = c.Value
			}
			if c.Name == "SESSDATA" && c.Expires > 0 {
				biliExpires = time.Unix(c.Expires, 0).Format("2006-01-02 15:04")
			}
		}
	}

	return map[string]any{
		"ok":            true,
		"time":          time.Now().Format(time.RFC3339),
		"data_dir":      a.cfg.DataDir,
		"total_jobs":    totalJobs,
		"running_jobs":  runningJobs,
		"ram":           ram,
		"rom":           rom,
		"cpu":           cpu,
		"network":       netInfo,
		"traffic_stats": trafficStats,
		"tools": map[string]string{
			"yt_dlp": a.cfg.YTDLP,
			"aria2":  a.cfg.Aria2,
			"biliup": a.cfg.Biliup,
			"llm":    a.cfg.DeepSeekModel,
		},
		"bilibili": map[string]string{
			"mid":     biliMid,
			"expires": biliExpires,
		},
	}
}

type cookieJSON struct {
	Domain         string  `json:"domain"`
	Path           string  `json:"path"`
	Name           string  `json:"name"`
	Value          string  `json:"value"`
	Expires        int64   `json:"expires"`
	ExpirationDate float64 `json:"expirationDate"`
	HTTPOnly       bool    `json:"httpOnly"`
	Secure         bool    `json:"secure"`
}

type cookieEnvelope struct {
	Cookies    []cookieJSON `json:"cookies"`
	CookieInfo struct {
		Cookies []cookieJSON `json:"cookies"`
	} `json:"cookie_info"`
}

func prepareCookies(src, dir string) (string, func(), error) {
	candidates := []string{src}
	if src != "/srv/y2b/youtube_cookies.txt" {
		candidates = append(candidates,
			"/srv/y2b/youtube_cookies.txt",
			"/srv/y2b/youtube_cookies.json",
			"/srv/y2b/cookies.txt",
			"/srv/y2b/yt_cookies.txt",
			"/srv/y2b/yt_cookies.json",
		)
	}
	for _, cand := range candidates {
		if cand == "" {
			continue
		}
		b, err := os.ReadFile(cand)
		if err != nil {
			continue
		}
		trimmed := bytes.TrimSpace(b)
		if len(trimmed) == 0 {
			continue
		}
		if trimmed[0] != '{' && trimmed[0] != '[' {
			return cand, func() {}, nil
		}
		var cs []cookieJSON
		if trimmed[0] == '[' {
			_ = json.Unmarshal(trimmed, &cs)
		} else {
			var env cookieEnvelope
			if err := json.Unmarshal(trimmed, &env); err == nil {
				cs = env.Cookies
				if len(cs) == 0 {
					cs = env.CookieInfo.Cookies
				}
			}
		}
		if len(cs) == 0 {
			continue
		}

		hasYouTubeOrGeneral := false
		for _, c := range cs {
			if c.Domain != "" && !strings.Contains(c.Domain, "bilibili") && !strings.Contains(c.Domain, "biligame") && !strings.Contains(c.Domain, "huasheng") {
				hasYouTubeOrGeneral = true
				break
			}
		}
		if !hasYouTubeOrGeneral {
			continue
		}

		tmp, err := os.CreateTemp(dir, ".cookies-*.txt")
		if err != nil {
			return "", func() {}, err
		}
		cleanup := func() { _ = os.Remove(tmp.Name()) }
		_, _ = io.WriteString(tmp, "# Netscape HTTP Cookie File\n")
		wrote := 0
		for _, c := range cs {
			if c.Domain == "" || c.Name == "" {
				continue
			}
			path := c.Path
			if path == "" {
				path = "/"
			}
			exp := c.Expires
			if exp == 0 && c.ExpirationDate > 0 {
				exp = int64(c.ExpirationDate)
			}
			domain := c.Domain
			if c.HTTPOnly {
				domain = "#HttpOnly_" + domain
			}
			fmt.Fprintf(tmp, "%s\t%s\t%s\t%s\t%d\t%s\t%s\n", domain, "TRUE", path, map[bool]string{true: "TRUE", false: "FALSE"}[c.Secure], exp, c.Name, strings.ReplaceAll(strings.ReplaceAll(c.Value, "\t", ""), "\n", ""))
			wrote++
		}
		if err := tmp.Close(); err != nil {
			cleanup()
			return "", func() {}, err
		}
		if wrote == 0 {
			cleanup()
			continue
		}
		return tmp.Name(), cleanup, nil
	}
	return "", func() {}, nil
}

type bccHeader struct {
	FontSize        float64   `json:"font_size"`
	FontColor       string    `json:"font_color"`
	BackgroundAlpha float64   `json:"background_alpha"`
	BackgroundColor string    `json:"background_color"`
	Stroke          string    `json:"Stroke"`
	Type            string    `json:"type"`
	Body            []bccItem `json:"body"`
}

type bccItem struct {
	From     float64 `json:"from"`
	To       float64 `json:"to"`
	Location int     `json:"location"`
	Content  string  `json:"content"`
}

func parseTimeToSeconds(s string) float64 {
	s = strings.TrimSpace(s)
	parts := strings.Split(s, ":")
	if len(parts) == 3 {
		h, _ := strconv.ParseFloat(parts[0], 64)
		m, _ := strconv.ParseFloat(parts[1], 64)
		secStr := strings.ReplaceAll(parts[2], ",", ".")
		sec, _ := strconv.ParseFloat(secStr, 64)
		return h*3600 + m*60 + sec
	} else if len(parts) == 2 {
		m, _ := strconv.ParseFloat(parts[0], 64)
		secStr := strings.ReplaceAll(parts[1], ",", ".")
		sec, _ := strconv.ParseFloat(secStr, 64)
		return m*60 + sec
	}
	return 0
}

func formatSRTTime(sec float64) string {
	totalMs := int(sec * 1000)
	h := totalMs / 3600000
	m := (totalMs % 3600000) / 60000
	s := (totalMs % 60000) / 1000
	ms := totalMs % 1000
	return fmt.Sprintf("%02d:%02d:%02d,%03d", h, m, s, ms)
}

func convertVttToSrtAndBcc(dir string) {
	matches, _ := filepath.Glob(filepath.Join(dir, "*.vtt"))
	reTime := regexp.MustCompile(`(\d{1,2}:\d{2}:\d{2}[\.,]\d{3}|\d{2}:\d{2}[\.,]\d{3})\s*-->\s*(\d{1,2}:\d{2}:\d{2}[\.,]\d{3}|\d{2}:\d{2}[\.,]\d{3})`)
	reTag := regexp.MustCompile(`<[^>]+>`)

	for _, vttPath := range matches {
		b, err := os.ReadFile(vttPath)
		if err != nil {
			continue
		}
		lines := strings.Split(string(b), "\n")
		var srtLines []string
		var bccItems []bccItem
		idx := 1

		for i := 0; i < len(lines); i++ {
			line := strings.TrimSpace(lines[i])
			if m := reTime.FindStringSubmatch(line); len(m) == 3 {
				fromSec := parseTimeToSeconds(m[1])
				toSec := parseTimeToSeconds(m[2])

				var textLines []string
				for j := i + 1; j < len(lines); j++ {
					tLine := strings.TrimSpace(lines[j])
					if tLine == "" || reTime.MatchString(tLine) {
						i = j - 1
						break
					}
					cleanText := reTag.ReplaceAllString(tLine, "")
					cleanText = strings.TrimSpace(cleanText)
					if cleanText != "" {
						textLines = append(textLines, cleanText)
					}
					if j == len(lines)-1 {
						i = j
					}
				}

				if len(textLines) > 0 {
					text := strings.Join(textLines, " ")
					bccItems = append(bccItems, bccItem{
						From:     fromSec,
						To:       toSec,
						Location: 2,
						Content:  text,
					})

					srtFrom := formatSRTTime(fromSec)
					srtTo := formatSRTTime(toSec)
					srtLines = append(srtLines, fmt.Sprintf("%d\n%s --> %s\n%s\n", idx, srtFrom, srtTo, text))
					idx++
				}
			}
		}

		base := strings.TrimSuffix(vttPath, ".vtt")
		if len(srtLines) > 0 {
			_ = os.WriteFile(base+".srt", []byte(strings.Join(srtLines, "\n")), 0644)
		}
		if len(bccItems) > 0 {
			bccData := bccHeader{
				FontSize:        0.4,
				FontColor:       "#FFFFFF",
				BackgroundAlpha: 0.5,
				BackgroundColor: "#9C27B0",
				Stroke:          "none",
				Type:            "header",
				Body:            bccItems,
			}
			if bccJSON, err := json.MarshalIndent(bccData, "", "  "); err == nil {
				_ = os.WriteFile(base+".bcc", bccJSON, 0644)
			}
		}
	}
}

func burnSubtitlesToVideos(ctx context.Context, dir string, videoFiles []string, onLine func(string)) ([]string, string, error) {
	var subFile string
	for _, pattern := range []string{"*zh-Hans*.vtt", "*zh*.vtt", "*zh-Hans*.srt", "*zh*.srt", "*en*.vtt", "*en*.srt", "*.vtt", "*.srt"} {
		matches, err := filepath.Glob(filepath.Join(dir, pattern))
		if err == nil && len(matches) > 0 {
			subFile = matches[0]
			break
		}
	}
	if subFile == "" {
		return videoFiles, "未找到字幕文件，跳过硬字幕压制\n", nil
	}

	var logs string
	var outVideos []string
	for idx, vf := range videoFiles {
		ext := filepath.Ext(vf)
		base := strings.TrimSuffix(vf, ext)
		burnedFile := base + ".burned.mp4"
		if strings.HasSuffix(base, ".burned") {
			outVideos = append(outVideos, vf)
			continue
		}

		escapedSub := strings.ReplaceAll(subFile, "\\", "/")
		escapedSub = strings.ReplaceAll(escapedSub, ":", "\\:")

		args := []string{
			"-y",
			"-progress", "pipe:2",
			"-nostats",
			"-i", vf,
			"-vf", fmt.Sprintf("subtitles='%s':force_style='FontSize=18,PrimaryColour=&H00FFFFFF,OutlineColour=&H00000000,BorderStyle=1,Outline=1.5,Shadow=1,MarginV=25'", escapedSub),
			"-c:v", "libx264",
			"-preset", "veryfast",
			"-crf", "20",
			"-c:a", "copy",
			burnedFile,
		}

		out, err := runCmdProgress(ctx, "ffmpeg", args, onLine)
		if err != nil {
			logs += fmt.Sprintf("[硬字幕压制失败 P%d] %v: %s\n", idx+1, err, string(out))
			outVideos = append(outVideos, vf)
		} else {
			logs += fmt.Sprintf("[硬字幕压制成功 P%d] -> %s\n", idx+1, filepath.Base(burnedFile))
			_ = os.Remove(vf)
			_ = os.Rename(burnedFile, vf)
			outVideos = append(outVideos, vf)
		}
	}
	return outVideos, logs, nil
}

func adjacentText(file, suffix string) string {
	base := strings.TrimSuffix(file, filepath.Ext(file)) + suffix
	if b, err := os.ReadFile(base); err == nil {
		if len(b) > 1<<20 {
			b = b[:1<<20]
		}
		return string(b)
	}
	// Fallback to directory scan (e.g. for split chapter files)
	dir := filepath.Dir(file)
	if matches, err := filepath.Glob(filepath.Join(dir, "*"+suffix)); err == nil && len(matches) > 0 {
		if b, err := os.ReadFile(matches[0]); err == nil {
			if len(b) > 1<<20 {
				b = b[:1<<20]
			}
			return string(b)
		}
	}
	return ""
}

func adjacentCover(file string) string {
	base := strings.TrimSuffix(file, filepath.Ext(file))
	for _, ext := range []string{".jpg", ".jpeg", ".png"} {
		p := base + ext
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}
	// If webp exists, convert to jpg for Bilibili compatibility
	webp := base + ".webp"
	if _, err := os.Stat(webp); err == nil {
		jpg := base + ".cover.jpg"
		if _, err := os.Stat(jpg); err == nil {
			return jpg
		}
		_ = exec.Command("ffmpeg", "-y", "-i", webp, jpg).Run()
		if _, err := os.Stat(jpg); err == nil {
			return jpg
		}
	}
	// Fallback to directory scan (e.g. for split chapter files)
	dir := filepath.Dir(file)
	for _, ext := range []string{".cover.jpg", ".jpg", ".jpeg", ".png"} {
		if matches, err := filepath.Glob(filepath.Join(dir, "*"+ext)); err == nil && len(matches) > 0 {
			return matches[0]
		}
	}
	if matches, err := filepath.Glob(filepath.Join(dir, "*.webp")); err == nil && len(matches) > 0 {
		webpFile := matches[0]
		jpgFile := strings.TrimSuffix(webpFile, ".webp") + ".cover.jpg"
		if _, err := os.Stat(jpgFile); err == nil {
			return jpgFile
		}
		_ = exec.Command("ffmpeg", "-y", "-i", webpFile, jpgFile).Run()
		if _, err := os.Stat(jpgFile); err == nil {
			return jpgFile
		}
	}
	return ""
}

func (a *App) createToken(user string) string {
	ts := strconv.FormatInt(time.Now().Unix(), 10)
	payload := user + ":" + ts
	mac := hmac.New(sha256.New, []byte(a.cfg.SecretKey))
	mac.Write([]byte(payload))
	sig := hex.EncodeToString(mac.Sum(nil))
	raw := payload + ":" + sig
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

func (a *App) verifyToken(tokenStr string) bool {
	if a.cfg.AdminPass == "" {
		return true // Auth disabled if no password set
	}
	raw, err := base64.RawURLEncoding.DecodeString(tokenStr)
	if err != nil {
		return false
	}
	parts := strings.Split(string(raw), ":")
	if len(parts) != 3 {
		return false
	}
	user := parts[0]
	tsStr := parts[1]
	sig := parts[2]

	if user != a.cfg.AdminUser {
		return false
	}
	ts, err := strconv.ParseInt(tsStr, 10, 64)
	if err != nil {
		return false
	}
	// Expire in 30 days
	if time.Now().Unix()-ts > 30*86400 || ts > time.Now().Unix()+300 {
		return false
	}

	payload := user + ":" + tsStr
	mac := hmac.New(sha256.New, []byte(a.cfg.SecretKey))
	mac.Write([]byte(payload))
	expectedSig := hex.EncodeToString(mac.Sum(nil))
	return hmac.Equal([]byte(sig), []byte(expectedSig))
}

func (a *App) isAuthorized(r *http.Request) bool {
	if a.cfg.AdminPass == "" {
		return true
	}
	// Check cookie
	if c, err := r.Cookie("y2b_token"); err == nil && c != nil && a.verifyToken(c.Value) {
		return true
	}
	// Check Authorization header
	authHeader := r.Header.Get("Authorization")
	if strings.HasPrefix(authHeader, "Bearer ") {
		token := strings.TrimPrefix(authHeader, "Bearer ")
		if a.verifyToken(token) {
			return true
		}
	}
	return false
}

type loginReq struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

func (a *App) loginHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		jsonResp(w, 405, map[string]string{"error": "method not allowed"})
		return
	}
	var req loginReq
	if decode(r, &req) != nil {
		jsonResp(w, 400, map[string]string{"error": "invalid JSON"})
		return
	}
	expectedUser := a.cfg.AdminUser
	expectedPass := a.cfg.AdminPass

	userMatch := subtle.ConstantTimeCompare([]byte(req.Username), []byte(expectedUser)) == 1
	passMatch := subtle.ConstantTimeCompare([]byte(req.Password), []byte(expectedPass)) == 1

	if !userMatch || !passMatch {
		jsonResp(w, 401, map[string]string{"error": "账号或密码错误"})
		return
	}

	token := a.createToken(req.Username)
	http.SetCookie(w, &http.Cookie{
		Name:     "y2b_token",
		Value:    token,
		Path:     "/",
		MaxAge:   30 * 86400,
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		Secure:   r.TLS != nil || r.Header.Get("X-Forwarded-Proto") == "https",
	})

	jsonResp(w, 200, map[string]any{
		"ok":       true,
		"token":    token,
		"username": req.Username,
	})
}

func (a *App) logoutHandler(w http.ResponseWriter, r *http.Request) {
	http.SetCookie(w, &http.Cookie{
		Name:     "y2b_token",
		Value:    "",
		Path:     "/",
		MaxAge:   -1,
		HttpOnly: true,
	})
	jsonResp(w, 200, map[string]any{"ok": true})
}

func (a *App) authStatusHandler(w http.ResponseWriter, r *http.Request) {
	authed := a.isAuthorized(r)
	jsonResp(w, 200, map[string]any{
		"auth_enabled":  a.cfg.AdminPass != "",
		"authenticated": authed,
		"username":      a.cfg.AdminUser,
	})
}

func serveIndex(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = io.WriteString(w, indexHTML)
}

func (a *App) handler(w http.ResponseWriter, r *http.Request) {
	// Keep accidental large uploads from consuming the service's small heap.
	if r.Body != nil {
		r.Body = http.MaxBytesReader(w, r.Body, 2<<20)
	}
	// Public web UI & Auth endpoints
	if r.Method == "GET" && (r.URL.Path == "/" || r.URL.Path == "") {
		serveIndex(w)
		return
	}
	if r.URL.Path == "/api/login" {
		a.loginHandler(w, r)
		return
	}
	if r.URL.Path == "/api/logout" {
		a.logoutHandler(w, r)
		return
	}
	if r.URL.Path == "/api/auth/status" {
		a.authStatusHandler(w, r)
		return
	}
	if r.Method == "GET" && r.URL.Path == "/health" {
		jsonResp(w, 200, map[string]any{"ok": true, "service": "y2b-go", "time": time.Now().UTC()})
		return
	}

	// Security Gate: Reject unauthenticated requests
	if !a.isAuthorized(r) {
		jsonResp(w, 401, map[string]any{
			"error":          "unauthorized",
			"login_required": true,
		})
		return
	}

	// Health & System Hardware (ROM/RAM/CPU/Network/Stats)
	if r.Method == "GET" && r.URL.Path == "/api/system" {
		jsonResp(w, 200, a.systemDiagnostics())
		return
	}
	if r.Method == "GET" && r.URL.Path == "/api/stats" {
		diag := a.systemDiagnostics()
		jsonResp(w, 200, map[string]any{
			"ok":            true,
			"network":       diag["network"],
			"traffic_stats": diag["traffic_stats"],
		})
		return
	}
	if r.Method == "POST" && r.URL.Path == "/api/stats/rescan" {
		a.loadStats()
		diag := a.systemDiagnostics()
		jsonResp(w, 200, map[string]any{
			"ok":            true,
			"traffic_stats": diag["traffic_stats"],
		})
		return
	}

	// File Server & Streamer
	if strings.HasPrefix(r.URL.Path, "/files/") {
		rel := strings.TrimPrefix(r.URL.Path, "/files/")
		root, _ := filepath.Abs(a.cfg.DataDir)
		target, _ := filepath.Abs(filepath.Join(root, filepath.Clean(rel)))
		if target != root && !strings.HasPrefix(target, root+string(os.PathSeparator)) {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		http.ServeFile(w, r, target)
		return
	}

	// Clean Media Packages & Temp Cleaning API
	if r.Method == "GET" && r.URL.Path == "/api/media" {
		a.listMediaHandler(w, r)
		return
	}
	if r.Method == "DELETE" && r.URL.Path == "/api/media" {
		a.deleteMediaHandler(w, r)
		return
	}
	if r.Method == "POST" && r.URL.Path == "/api/files/clean-temp" {
		a.cleanTempHandler(w, r)
		return
	}

	// AI Enhancement
	if r.Method == "POST" && r.URL.Path == "/api/ai/enhance" {
		a.aiEnhanceHandler(w, r)
		return
	}

	// Channel Monitoring API
	if r.URL.Path == "/api/channels" {
		if r.Method == "GET" {
			a.getChannelsHandler(w, r)
			return
		}
		if r.Method == "POST" {
			a.createChannelHandler(w, r)
			return
		}
		jsonResp(w, 405, map[string]string{"error": "method not allowed"})
		return
	}
	if strings.HasPrefix(r.URL.Path, "/api/channels/") {
		parts := strings.Split(strings.Trim(strings.TrimPrefix(r.URL.Path, "/api/channels/"), "/"), "/")
		id := parts[0]
		if r.Method == "DELETE" && len(parts) == 1 {
			a.deleteChannelHandler(w, r, id)
			return
		}
		if r.Method == "POST" && len(parts) == 2 && parts[1] == "toggle" {
			a.toggleChannelHandler(w, r, id)
			return
		}
		if r.Method == "POST" && len(parts) == 2 && parts[1] == "sync" {
			a.syncChannelHandler(w, r, id)
			return
		}
		jsonResp(w, 405, map[string]string{"error": "method not allowed"})
		return
	}

	// Jobs List & Batch Clear
	if r.Method == "GET" && r.URL.Path == "/api/jobs" {
		jsonResp(w, 200, a.listJobs(r.URL.Query().Get("status"), r.URL.Query().Get("kind")))
		return
	}
	if r.Method == "POST" && r.URL.Path == "/api/jobs/clear" {
		cleared := a.clearFinishedJobs()
		jsonResp(w, 200, map[string]any{"ok": true, "deleted": cleared})
		return
	}

	// Job by ID Operations
	if strings.HasPrefix(r.URL.Path, "/api/jobs/") {
		parts := strings.Split(strings.Trim(strings.TrimPrefix(r.URL.Path, "/api/jobs/"), "/"), "/")
		id := parts[0]

		if r.Method == "GET" && len(parts) == 2 && parts[1] == "logs" {
			a.mu.RLock()
			j := a.jobs[id]
			a.mu.RUnlock()
			if j == nil {
				jsonResp(w, 404, map[string]string{"error": "job not found"})
			} else {
				jsonResp(w, 200, map[string]string{"id": id, "logs": j.Logs, "status": j.Status, "step": j.Step})
			}
			return
		}

		if r.Method == "DELETE" && len(parts) == 1 {
			if err := a.deleteJob(id); err != nil {
				jsonResp(w, 409, map[string]string{"error": err.Error()})
			} else {
				jsonResp(w, 200, map[string]any{"ok": true, "id": id})
			}
			return
		}
		if r.Method == "POST" && len(parts) == 2 && parts[1] == "retry" {
			j, err := a.retryJob(id)
			if err != nil {
				jsonResp(w, 409, map[string]string{"error": err.Error()})
			} else {
				jsonResp(w, 202, j)
			}
			return
		}
		if r.Method == "POST" && len(parts) == 2 && parts[1] == "cancel" {
			if err := a.cancelJob(id); err != nil {
				jsonResp(w, 409, map[string]string{"error": err.Error()})
			} else {
				jsonResp(w, 200, map[string]any{"ok": true, "id": id})
			}
			return
		}
		if r.Method != "GET" || len(parts) != 1 {
			jsonResp(w, 405, map[string]string{"error": "method not allowed"})
			return
		}
		a.mu.RLock()
		j := a.jobs[id]
		a.mu.RUnlock()
		if j == nil {
			jsonResp(w, 404, map[string]string{"error": "job not found"})
		} else {
			jsonResp(w, 200, j)
		}
		return
	}

	// Task Creation Endpoints
	if r.Method != "POST" {
		jsonResp(w, 405, map[string]string{"error": "method not allowed"})
		return
	}
	switch r.URL.Path {
	case "/api/youtube/download":
		a.youtube(w, r)
	case "/api/magnet/download":
		a.magnet(w, r)
	case "/api/biliup/upload":
		a.upload(w, r)
	case "/api/pipeline":
		a.pipeline(w, r)
	default:
		jsonResp(w, 404, map[string]string{"error": "not found"})
	}
}

func main() {
	// Set Go runtime memory limit and ultra-aggressive GC to prevent RAM spikes
	debug.SetMemoryLimit(64 * 1024 * 1024) // 64MB ceiling
	debug.SetGCPercent(15)                 // Aggressive GC threshold (15% heap growth)
	startMemoryWatchdog()

	a := &App{
		cfg:           loadConfig(),
		jobs:          map[string]*Job{},
		downloadSlots: make(chan struct{}, 1),
		uploadSlots:   make(chan struct{}, 1),
		channels:      map[string]*MonitoredChannel{},
	}
	if err := os.MkdirAll(a.cfg.DataDir, 0750); err != nil {
		panic(err)
	}
	a.loadJobs()
	a.loadChannels()
	a.loadStats()
	a.startChannelWatcher(context.Background())
	a.startNetworkSampler(context.Background())

	s := &http.Server{
		Addr:              a.cfg.Addr,
		Handler:           http.HandlerFunc(a.handler),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       60 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	fmt.Printf("y2b-go listening on %s (data: %s)\n", a.cfg.Addr, a.cfg.DataDir)
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-stop
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = s.Shutdown(ctx)
	}()
	if err := s.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		fmt.Fprintf(os.Stderr, "y2b-go stopped unexpectedly: %v\n", err)
		os.Exit(1)
	}
}
