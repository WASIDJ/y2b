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
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
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

var (
	Version         = "1.2.0"
	BuildCommit     = "dev"
	BuildTime       = "unknown"
	serverStartTime = time.Now()
)


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
	DownloadTimeout  time.Duration
	UploadTimeout    time.Duration
	QueueWaitTimeout time.Duration
	MagnetTimeout    time.Duration
	BTListenPort     string
	AutoRetryMax     int
	AutoRetryBase    time.Duration
	ReviewInterval   time.Duration
	ReviewRepairMax  int
	MinFreeDiskGB    float64
	MaxJobDiskGB     float64
	SubmitEndpoint   string
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
	ID              string       `json:"id"`
	Kind            string       `json:"kind"` // "youtube", "magnet", "biliup", "pipeline"
	Title           string       `json:"title,omitempty"`
	Status          string       `json:"status"` // "queued", "running", "done", "failed", "canceled"
	Step            string       `json:"step,omitempty"`
	Error           string       `json:"error,omitempty"`
	Created         time.Time    `json:"created"`
	Started         time.Time    `json:"started,omitempty"`
	Finished        time.Time    `json:"finished,omitempty"`
	Input           any          `json:"input,omitempty"`
	Output          any          `json:"output,omitempty"`
	Logs            string       `json:"logs,omitempty"`
	Progress        *JobProgress `json:"progress,omitempty"`
	FailureCategory string       `json:"failure_category,omitempty"`
	AutoRetryCount  int          `json:"auto_retry_count,omitempty"`
	NextRetryAt     time.Time    `json:"next_retry_at,omitempty"`
	ReviewState     string       `json:"review_state,omitempty"`
	ReviewError     string       `json:"review_error,omitempty"`
	ReviewCheckedAt time.Time    `json:"review_checked_at,omitempty"`
	ReviewRepairs   int          `json:"review_repairs,omitempty"`
	ctx             context.Context
	cancelFunc      context.CancelFunc
	retry           func(*Job)
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

type loginAttempt struct {
	count    int
	lockedTo time.Time
}

type mediaScanCache struct {
	cachedAt time.Time
	pkgs     []MediaPackage
}

type App struct {
	cfg                 Config
	mu                  sync.RWMutex
	jobs                map[string]*Job
	order               []string
	downloadSlots       chan struct{}
	uploadSlots         chan struct{}
	uploadCooldownUntil time.Time
	cmu                 sync.RWMutex
	channels            map[string]*MonitoredChannel
	channelOrder        []string
	smu                 sync.RWMutex
	stats               AppStats
	netStats            NetworkStats
	reviewMu            sync.Mutex
	loginMu             sync.Mutex
	loginAttempts       map[string]*loginAttempt
	mediaCacheMu        sync.Mutex
	mediaCache          mediaScanCache
}

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

// loadOrCreateSecretKey reads the HMAC signing key from <dataDir>/.secret_key.
// If the file does not exist, a cryptographically random 32-byte key is
// generated and stored so it survives service restarts. This avoids shipping a
// fixed default key in the source code while keeping tokens valid across
// process restarts.
func loadOrCreateSecretKey(dataDir string) string {
	keyFile := filepath.Join(dataDir, ".secret_key")
	if b, err := os.ReadFile(keyFile); err == nil && len(b) >= 32 {
		return strings.TrimSpace(string(b))
	}
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return hex.EncodeToString([]byte(fmt.Sprintf("y2b-auto-%d", time.Now().UnixNano())))
	}
	key := hex.EncodeToString(raw)
	_ = os.MkdirAll(dataDir, 0750)
	_ = os.WriteFile(keyFile, []byte(key), 0600)
	return key
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

	dataDir := env("Y2B_DATA", "/srv/y2b/data")
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
		adminPass = os.Getenv("Y2B_ADMIN_PASSWORD")
	}
	if adminPass == "" {
		fmt.Fprintln(os.Stderr, "[y2b] WARNING: WEB_PASSWORD is not set. Authentication is DISABLED. "+
			"Set WEB_PASSWORD or Y2B_ADMIN_PASSWORD in /etc/y2b.env to protect the web console.")
	}
	secretKey := os.Getenv("WEB_SECRET_KEY")
	if secretKey == "" {
		secretKey = loadOrCreateSecretKey(dataDir)
	}
	uploadTimeout := 4 * time.Hour
	if raw := os.Getenv("Y2B_UPLOAD_TIMEOUT"); raw != "" {
		if parsed, err := time.ParseDuration(raw); err == nil && parsed > 0 {
			uploadTimeout = parsed
		}
	}
	// A queued job must not be discarded merely because another large torrent
	// is downloading. The queue is intentionally serialized on this host.
	// Zero means wait until the job is explicitly canceled or the process
	// context ends; queue length must not turn into a false download failure.
	queueWaitTimeout := time.Duration(0)
	if raw := os.Getenv("Y2B_QUEUE_WAIT_TIMEOUT"); raw != "" {
		if parsed, err := time.ParseDuration(raw); err == nil && parsed > 0 {
			queueWaitTimeout = parsed
		}
	}
	// Torrent metadata/peer discovery can legitimately take longer than a
	// short HTTP request. Keep this bounded, but do not fail large jobs after
	// the old 30-minute window.
	magnetTimeout := 6 * time.Hour
	downloadTimeout := 2 * time.Hour
	if raw := os.Getenv("Y2B_DOWNLOAD_TIMEOUT"); raw != "" {
		if parsed, err := time.ParseDuration(raw); err == nil && parsed > 0 {
			downloadTimeout = parsed
		}
	}
	if raw := os.Getenv("Y2B_MAGNET_TIMEOUT"); raw != "" {
		if parsed, err := time.ParseDuration(raw); err == nil && parsed > 0 {
			magnetTimeout = parsed
		}
	}
	btListenPort := env("Y2B_BT_LISTEN_PORT", "51413")
	// Zero means unlimited automatic retries. A transient Bilibili cooldown or
	// a slow/dead torrent must not turn into a permanently abandoned job.
	autoRetryMax := 0
	if raw := os.Getenv("Y2B_AUTO_RETRY_MAX"); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed >= 0 && parsed <= 10 {
			autoRetryMax = parsed
		}
	}
	autoRetryBase := 30 * time.Second
	if raw := os.Getenv("Y2B_AUTO_RETRY_BASE"); raw != "" {
		if parsed, err := time.ParseDuration(raw); err == nil && parsed > 0 {
			autoRetryBase = parsed
		}
	}
	reviewInterval := 10 * time.Minute
	if raw := os.Getenv("Y2B_REVIEW_INTERVAL"); raw != "" {
		if parsed, err := time.ParseDuration(raw); err == nil && parsed > 0 {
			reviewInterval = parsed
		}
	}
	reviewRepairMax := 2
	if raw := os.Getenv("Y2B_REVIEW_REPAIR_MAX"); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed >= 0 && parsed <= 5 {
			reviewRepairMax = parsed
		}
	}
	minFreeDiskGB := 5.0
	if raw := os.Getenv("Y2B_MIN_FREE_GIB"); raw != "" {
		if parsed, err := strconv.ParseFloat(raw, 64); err == nil && parsed > 0 {
			minFreeDiskGB = parsed
		}
	}
	maxJobDiskGB := 40.0
	if raw := os.Getenv("Y2B_MAX_JOB_DISK_GB"); raw != "" {
		if parsed, err := strconv.ParseFloat(raw, 64); err == nil && parsed > 0 {
			maxJobDiskGB = parsed
		}
	}
	submitEndpoint := env("Y2B_BILIUP_SUBMIT_ENDPOINT", "b-cut-android")

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
		DownloadTimeout:  downloadTimeout,
		UploadTimeout:    uploadTimeout,
		QueueWaitTimeout: queueWaitTimeout,
		MagnetTimeout:    magnetTimeout,
		BTListenPort:     btListenPort,
		AutoRetryMax:     autoRetryMax,
		AutoRetryBase:    autoRetryBase,
		ReviewInterval:   reviewInterval,
		ReviewRepairMax:  reviewRepairMax,
		MinFreeDiskGB:    minFreeDiskGB,
		MaxJobDiskGB:     maxJobDiskGB,
		SubmitEndpoint:   submitEndpoint,
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
		ID              string       `json:"id"`
		Kind            string       `json:"kind"`
		Status          string       `json:"status"`
		Step            string       `json:"step,omitempty"`
		Error           string       `json:"error,omitempty"`
		Created         time.Time    `json:"created"`
		Started         time.Time    `json:"started,omitempty"`
		Finished        time.Time    `json:"finished,omitempty"`
		Input           any          `json:"input,omitempty"`
		Output          any          `json:"output,omitempty"`
		Logs            string       `json:"logs,omitempty"`
		Progress        *JobProgress `json:"progress,omitempty"`
		FailureCategory string       `json:"failure_category,omitempty"`
		AutoRetryCount  int          `json:"auto_retry_count,omitempty"`
		NextRetryAt     time.Time    `json:"next_retry_at,omitempty"`
		ReviewState     string       `json:"review_state,omitempty"`
		ReviewError     string       `json:"review_error,omitempty"`
		ReviewCheckedAt time.Time    `json:"review_checked_at,omitempty"`
		ReviewRepairs   int          `json:"review_repairs,omitempty"`
	}

	list := make([]persistedJob, 0, len(a.order))
	for _, oid := range a.order {
		if j := a.jobs[oid]; j != nil {
			list = append(list, persistedJob{
				ID:              j.ID,
				Kind:            j.Kind,
				Status:          j.Status,
				Step:            j.Step,
				Error:           j.Error,
				Created:         j.Created,
				Started:         j.Started,
				Finished:        j.Finished,
				Input:           j.Input,
				Output:          j.Output,
				Logs:            j.Logs,
				Progress:        j.Progress,
				FailureCategory: j.FailureCategory,
				AutoRetryCount:  j.AutoRetryCount,
				NextRetryAt:     j.NextRetryAt,
				ReviewState:     j.ReviewState,
				ReviewError:     j.ReviewError,
				ReviewCheckedAt: j.ReviewCheckedAt,
				ReviewRepairs:   j.ReviewRepairs,
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
		j.ctx, j.cancelFunc = context.WithCancel(context.Background())
		if j.Status == "running" || j.Status == "queued" {
			j.Status = "canceled"
			j.Error = "服务重启中断"
			j.Finished = time.Now()
		} else if j.Status == "failed" {
			category := classifyFailure(j.Error, j.Logs)
			j.FailureCategory = category
			if category == "missing_media" && j.Kind == "biliup" {
				j.NextRetryAt = time.Time{}
				var up uploadReq
				if b, err := json.Marshal(j.Input); err == nil {
					_ = json.Unmarshal(b, &up)
				}
				src := up.OriginalURL
				if src == "" {
					src = up.Source
				}
				if !validYouTube(src) && !validTorrentOrMagnet(src) {
					j.Status = "canceled"
					j.Step = "已取消 (媒体文件缺失且无原始下载链接)"
				}
			} else if category == "disk_full" {
				j.NextRetryAt = time.Time{}
				j.Step = "磁盘空间不足，等待手动清理后重试"
			} else if category == "upload_rate_limit" {
				j.Step = "等待B站限流/风控解除 (可人工验证或次日自动刷新)"
			}
		}
		if j.Title == "" {
			j.Title = extractJobTitle(j.Kind, j.Input)
			if outMap := outputMap(j.Output); outMap != nil {
				if t, ok := outMap["title"].(string); ok && strings.TrimSpace(t) != "" {
					j.Title = strings.TrimSpace(t)
				}
			}
		}
		a.jobs[j.ID] = j
		a.order = append(a.order, j.ID)
	}
}

func jobSourceKey(kind string, input any) string {
	b, _ := json.Marshal(input)
	var fields map[string]any
	_ = json.Unmarshal(b, &fields)
	source := ""
	for _, key := range []string{"url", "URL", "magnet", "Magnet", "file", "File"} {
		if value, ok := fields[key].(string); ok && strings.TrimSpace(value) != "" {
			source = strings.TrimSpace(value)
			break
		}
	}
	if strings.HasPrefix(strings.ToLower(source), "magnet:") {
		if parsed, err := url.Parse(source); err == nil {
			if hash := strings.ToLower(parsed.Query().Get("xt")); hash != "" {
				return kind + "|magnet|" + hash
			}
		}
	}
	if source != "" {
		return kind + "|source|" + strings.TrimRight(source, "/")
	}
	return kind + "|input|" + string(b)
}

// compactDuplicateJobs removes terminal records created by the old retry
// implementation. A completed record wins; otherwise the newest terminal
// record represents the logical video. Active records are never deleted.
func (a *App) compactDuplicateJobs() int {
	a.mu.Lock()
	groups := make(map[string][]*Job)
	for _, oid := range a.order {
		if j := a.jobs[oid]; j != nil {
			groups[jobSourceKey(j.Kind, j.Input)] = append(groups[jobSourceKey(j.Kind, j.Input)], j)
		}
	}
	remove := make(map[string]bool)
	for _, jobs := range groups {
		if len(jobs) < 2 {
			continue
		}
		var keep *Job
		for _, j := range jobs {
			if j.Status == "queued" || j.Status == "running" {
				continue
			}
			if keep == nil ||
				(j.Status == "done" && keep.Status != "done") ||
				(j.Status == keep.Status && j.Created.After(keep.Created)) {
				keep = j
			}
		}
		if keep == nil {
			continue
		}
		for _, j := range jobs {
			if j.ID != keep.ID && (j.Status == "failed" || j.Status == "canceled" || j.Status == "done") {
				remove[j.ID] = true
			}
		}
	}
	for oid := range remove {
		delete(a.jobs, oid)
	}
	a.removeJobIDsLocked(remove)
	count := len(remove)
	a.mu.Unlock()
	if count > 0 {
		a.saveJobs()
	}
	return count
}

// extractJobTitle extracts a clean, human-readable title from job input or URL.
func extractJobTitle(kind string, input any) string {
	if input == nil {
		return ""
	}
	if b, err := json.Marshal(input); err == nil {
		var p struct {
			Title     string   `json:"title"`
			URL       string   `json:"url"`
			Magnet    string   `json:"magnet"`
			File      string   `json:"file"`
			Files     []string `json:"files"`
			ResumeDir string   `json:"resume_dir"`
		}
		if json.Unmarshal(b, &p) == nil {
			if strings.TrimSpace(p.Title) != "" {
				return strings.TrimSpace(p.Title)
			}
			rawURL := p.URL
			if rawURL == "" {
				rawURL = p.Magnet
			}
			if strings.HasPrefix(strings.ToLower(rawURL), "magnet:") {
				if dn := extractMagnetDN(rawURL); dn != "" {
					return dn
				}
			} else if strings.Contains(rawURL, "youtube.com") || strings.Contains(rawURL, "youtu.be") {
				return extractYouTubeCleanTitle(rawURL)
			}
			if len(p.Files) > 0 && p.Files[0] != "" {
				return filepath.Base(p.Files[0])
			}
			if p.File != "" {
				return filepath.Base(p.File)
			}
			if p.ResumeDir != "" {
				return filepath.Base(p.ResumeDir)
			}
		}
	}
	return ""
}

func extractMagnetDN(rawURL string) string {
	if u, err := url.Parse(rawURL); err == nil {
		if dn := u.Query().Get("dn"); dn != "" {
			return strings.TrimSpace(dn)
		}
		if xt := u.Query().Get("xt"); xt != "" {
			hash := strings.TrimPrefix(xt, "urn:btih:")
			if len(hash) > 12 {
				hash = hash[:12] + "..."
			}
			return "磁力: " + hash
		}
	}
	re := regexp.MustCompile(`(?i)[?&]dn=([^&]+)`)
	if m := re.FindStringSubmatch(rawURL); len(m) > 1 {
		if decoded, err := url.QueryUnescape(m[1]); err == nil {
			return strings.TrimSpace(decoded)
		}
		return strings.TrimSpace(m[1])
	}
	return ""
}

func extractYouTubeCleanTitle(rawURL string) string {
	if u, err := url.Parse(rawURL); err == nil {
		if list := u.Query().Get("list"); list != "" {
			if len(list) > 12 {
				list = list[:12] + "..."
			}
			return "YouTube 播放列表 (" + list + ")"
		}
		if v := u.Query().Get("v"); v != "" {
			return "YouTube 视频 (" + v + ")"
		}
	}
	if strings.Contains(rawURL, "youtu.be/") {
		parts := strings.Split(rawURL, "youtu.be/")
		if len(parts) > 1 {
			id := strings.Split(parts[1], "?")[0]
			return "YouTube 视频 (" + id + ")"
		}
	}
	return "YouTube 视频"
}

func (a *App) add(kind string, input any) *Job {
	ctx, cancel := context.WithCancel(context.Background())
	j := &Job{
		ID:         id(),
		Kind:       kind,
		Title:      extractJobTitle(kind, input),
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
	shouldAutoRetry := false
	var retryDelay time.Duration
	a.mu.Lock()
	j.Status = status
	j.Error = err
	j.Output = out
	if out != nil {
		if outMap := outputMap(out); outMap != nil {
			if t, ok := outMap["title"].(string); ok && strings.TrimSpace(t) != "" {
				j.Title = strings.TrimSpace(t)
			}
		}
	}
	if logs != "" {
		if len(logs) > 64*1024 {
			logs = logs[len(logs)-64*1024:] // Keep latest 64KB to avoid RAM growth
		}
		j.Logs = logs
	}
	if status == "failed" {
		j.FailureCategory = classifyFailure(err, logs)
		if j.FailureCategory == "upload_rate_limit" {
			cooldownUntil := time.Now().Add(30 * time.Minute)
			if cooldownUntil.After(a.uploadCooldownUntil) {
				a.uploadCooldownUntil = cooldownUntil
			}
		}
		if autoRetryAllowed(a.cfg.AutoRetryMax, j.AutoRetryCount, j.FailureCategory) {
			j.AutoRetryCount++
			shouldAutoRetry = true
			retryDelay = a.retryDelayFor(j.FailureCategory, j.AutoRetryCount)
			j.NextRetryAt = time.Now().Add(retryDelay)
		}
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
		if outMap, ok := out.(map[string]any); ok {
			if rs, hasRS := outMap["review_state"].(string); hasRS && j.ReviewState == "" {
				j.ReviewState = rs
			}
		}
	} else if status == "failed" {
		j.Finished = time.Now()
		switch j.FailureCategory {
		case "upload_rate_limit":
			j.Step = "等待B站限流/风控解除 (可人工验证或次日自动刷新)"
		case "disk_full":
			j.Step = "磁盘空间不足，等待手动清理后重试"
		case "auth_failed":
			j.Step = "B站登录鉴权失效或重复稿件，请更新 cookies.json"
		default:
			j.Step = "失败"
		}
	} else if status == "canceled" {
		j.Finished = time.Now()
		j.Step = "已取消"
	}
	a.mu.Unlock()
	a.saveJobs()
	if shouldAutoRetry {
		a.scheduleAutoRetry(j, retryDelay)
	}
	// Reclaim disk space immediately after any terminal state so completed
	// media does not linger until the next service restart.
	if status == "done" || status == "failed" || status == "canceled" {
		go func() {
			a.cleanupCompletedJobMedia()
			a.cleanupOrphanedMedia()
		}()
	}
}

func (a *App) retryDelayFor(category string, retryNo int) time.Duration {
	if retryNo < 1 {
		retryNo = 1
	}
	base := a.cfg.AutoRetryBase
	if category == "upload_rate_limit" {
		// Rate limiting: pause for 30 minutes between attempts so operator can
		// verify on Bilibili or next-day quota refreshes automatically.
		return 30 * time.Minute
	}
	if category == "youtube_bot_challenge" {
		// YouTube session rate limits typically advise waiting up to an hour.
		// Space retries out to avoid persistent bans: 5m, 15m, 30m, 45m...
		if retryNo > 4 {
			retryNo = 4
		}
		return time.Duration(retryNo*15) * time.Minute
	}
	if retryNo > 5 {
		retryNo = 5
	}
	return base * time.Duration(1<<(retryNo-1))
}


// classifyFailure turns noisy external-tool output into a stable category that
// the UI and the bounded recovery loop can act on.
func classifyFailure(err, logs string) string {
	s := strings.ToLower(err + "\n" + logs)
	switch {
	case strings.Contains(s, "队列等待超时"), strings.Contains(s, "queue wait timeout"):
		return "queue_timeout"
	case strings.Contains(s, "code -663"), strings.Contains(s, "code: -663"), strings.Contains(s, `"code":-663`), strings.Contains(s, `"code": -663`),
		strings.Contains(s, "鉴权失败"), strings.Contains(s, "登录凭证失效"),
		strings.Contains(s, "code -101"), strings.Contains(s, "code: -101"), strings.Contains(s, `"code":-101`), strings.Contains(s, `"code": -101`),
		strings.Contains(s, "code -400"), strings.Contains(s, "code: -400"), strings.Contains(s, `"code":-400`), strings.Contains(s, `"code": -400`),
		strings.Contains(s, "重复稿件"), strings.Contains(s, "code 21070"), strings.Contains(s, "code: 21070"), strings.Contains(s, "code 21071"), strings.Contains(s, "code: 21071"),
		strings.Contains(s, "code 21016"), strings.Contains(s, "code: 21016"), strings.Contains(s, "code 21017"), strings.Contains(s, "code: 21017"), strings.Contains(s, "code 21018"), strings.Contains(s, "code: 21018"):
		return "auth_failed"
	case strings.Contains(s, "code 406"), strings.Contains(s, `"code":406`),
		strings.Contains(s, "code 601"), strings.Contains(s, `"code":601`),
		strings.Contains(s, "code 21564"), strings.Contains(s, `"code":21564`),
		strings.Contains(s, "code 21085"), strings.Contains(s, `"code":21085`),
		strings.Contains(s, "上传视频过快"), strings.Contains(s, "upload too fast"),
		strings.Contains(s, "上传频次"), strings.Contains(s, "投稿频次"),
		strings.Contains(s, "验证码"), strings.Contains(s, "geetest"),
		strings.Contains(s, "限流"), strings.Contains(s, "风控"),
		strings.Contains(s, "upload rate limit"), strings.Contains(s, "biliup rate limit"):
		return "upload_rate_limit"
	case strings.Contains(s, "sign in to confirm you're not a bot"),
		strings.Contains(s, "sign in to confirm you’re not a bot"),
		strings.Contains(s, "sign in to confirm you are not a bot"),
		strings.Contains(s, "use --cookies-from-browser"),
		strings.Contains(s, "confirming you're not a bot"),
		strings.Contains(s, "confirming you’re not a bot"),
		strings.Contains(s, "confirm you're not a bot"),
		strings.Contains(s, "confirm you’re not a bot"),
		strings.Contains(s, "youtube bot challenge"),
		strings.Contains(s, "bot challenge"),
		strings.Contains(s, "http error 429"),
		strings.Contains(s, "too many requests"),
		strings.Contains(s, "rate-limited by youtube"),
		strings.Contains(s, "this content isn't available, try again later"):
		return "youtube_bot_challenge"
	case strings.Contains(s, "no such file or directory"),
		strings.Contains(s, "os error 2"),
		strings.Contains(s, "missing_media"):
		return "missing_media"
	case strings.Contains(s, "磁盘空间不足"),
		strings.Contains(s, "no space left on device"),
		strings.Contains(s, "not enough disk space"),
		strings.Contains(s, "disk full"),
		strings.Contains(s, "quota_exceeded"),
		strings.Contains(s, "安全配额上限"),
		strings.Contains(s, "exit status 9"):
		// aria2c exit status 9 = Not Enough Disk Space; disk_full is never
		// auto-retried — retrying immediately just wastes resources until
		// the user frees disk space manually.
		return "disk_full"
	case strings.Contains(s, "magnet_timeout"),
		strings.Contains(s, "bt 下载超过"):
		return "magnet_timeout"
	case strings.Contains(s, "dead_seed"),
		strings.Contains(s, "未发现可用做种"),
		strings.Contains(s, "bt-stop-timeout"),
		strings.Contains(s, "exit status 7"):
		return "dead_seed"
	case strings.Contains(s, "context canceled"), strings.Contains(s, "已取消"):
		return "canceled"
	default:
		return "unknown"
	}
}

func isAutoRetryableCategory(category string) bool {
	return category == "queue_timeout" || category == "upload_rate_limit" ||
		category == "magnet_timeout" || category == "youtube_bot_challenge"
}

// uploadRateLimitMaxRetries is the hard ceiling for upload_rate_limit retries
// (~48 attempts × 30 min ≈ 24 hours). Beyond this the job stays failed and
// requires manual verification (B站 account check, cookie refresh, etc.).
const uploadRateLimitMaxRetries = 48

func autoRetryAllowed(max, count int, category string) bool {
	// Rate limiting is NOT a permanent failure: keep task pending with periodic
	// retries, but cap at uploadRateLimitMaxRetries to avoid infinite loops.
	// After the cap the job stays failed and requires manual intervention.
	if category == "upload_rate_limit" {
		return count < uploadRateLimitMaxRetries
	}
	return (max <= 0 || count < max) && isAutoRetryableCategory(category)
}

func (a *App) scheduleAutoRetry(j *Job, delay time.Duration) {
	if delay <= 0 {
		delay = time.Second
	}
	go func(jobID string, retryNo int) {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		<-timer.C
		if retried, err := a.retryJob(jobID); err == nil {
			fmt.Printf("auto-retry #%d scheduled job %s as %s\n", retryNo, jobID, retried.ID)
		}
	}(j.ID, j.AutoRetryCount)
}

// recoverTransientJobs is called after loading persisted state so failures
// from before this recovery logic existed also get one bounded retry policy.
func (a *App) recoverTransientJobs() {
	// Do not resurrect an entire old backlog on every service restart. That
	// creates a retry storm (and immediately triggers Bilibili's 406 limit).
	// Jobs that failed recently are safe to recover; older jobs keep their
	// local media and can be retried deliberately after the cooldown.
	now := time.Now()
	recoveryCutoff := now.Add(-30 * time.Minute)
	a.mu.RLock()
	type recovery struct {
		job   *Job
		delay time.Duration
		bump  bool
	}
	failed := make([]recovery, 0)
	for _, oid := range a.order {
		j := a.jobs[oid]
		if j != nil && j.Status == "failed" && !j.Finished.IsZero() && (j.Finished.After(recoveryCutoff) || !j.NextRetryAt.IsZero()) {
			category := classifyFailure(j.Error, j.Logs)
			if autoRetryAllowed(a.cfg.AutoRetryMax, j.AutoRetryCount, category) {
				retryNo := j.AutoRetryCount
				if retryNo < 1 {
					retryNo = 1
				}
				delay := a.retryDelayFor(category, retryNo)
				bump := false
				if !j.NextRetryAt.IsZero() {
					delay = time.Until(j.NextRetryAt)
					if delay < 0 {
						delay = 0
					}
					bump = false
				}
				failed = append(failed, recovery{job: j, delay: delay, bump: bump})
			}
		}
	}
	a.mu.RUnlock()
	for _, item := range failed {
		if item.bump {
			a.mu.Lock()
			item.job.AutoRetryCount++
			item.job.NextRetryAt = time.Now().Add(item.delay)
			a.mu.Unlock()
		}
		a.scheduleAutoRetry(item.job, item.delay)
	}
	if len(failed) > 0 {
		a.saveJobs()
		fmt.Printf("scheduled %d transient failed jobs for bounded recovery\n", len(failed))
	}
}

// recoverInterruptedJobs resumes work that was interrupted by a service
// restart. Intentional user cancellations do not use this error marker.
func (a *App) recoverInterruptedJobs() {
	a.mu.RLock()
	ids := make([]string, 0)
	for _, oid := range a.order {
		j := a.jobs[oid]
		if j != nil && j.Status == "canceled" && j.Error == "服务重启中断" {
			ids = append(ids, j.ID)
		}
	}
	a.mu.RUnlock()
	if len(ids) == 0 {
		return
	}
	go func() {
		time.Sleep(10 * time.Second)
		for _, jobID := range ids {
			if _, err := a.retryJob(jobID); err != nil {
				fmt.Printf("startup recovery skipped job %s: %v\n", jobID, err)
			}
			// Space out job resumes to prevent contention on download/upload slots
			time.Sleep(2 * time.Second)
		}
	}()
	fmt.Printf("scheduled %d interrupted jobs for automatic recovery\n", len(ids))
}

// retryWatchdog is the durable backstop for in-memory retry timers. It makes
// a due retry self-healing even if a timer was lost during a restart or a
// transient runtime failure.
func (a *App) retryWatchdog(ctx context.Context) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := time.Now()
			a.mu.RLock()
			ids := make([]string, 0)
			for _, oid := range a.order {
				j := a.jobs[oid]
				category := ""
				if j != nil {
					category = classifyFailure(j.Error, j.Logs)
				}
				if j != nil && j.Status == "failed" && !j.NextRetryAt.IsZero() && !j.NextRetryAt.After(now) && autoRetryAllowed(a.cfg.AutoRetryMax, j.AutoRetryCount, category) {
					ids = append(ids, j.ID)
				}
			}
			a.mu.RUnlock()
			for _, jobID := range ids {
				if _, err := a.retryJob(jobID); err != nil {
					fmt.Printf("retry watchdog skipped job %s: %v\n", jobID, err)
				}
			}
		}
	}
}

// diskRecoveryWatchdog periodically checks whether disk space has recovered
// enough to resume disk_full jobs. It runs every 5 minutes and re-queues any
// failed job whose failure category is disk_full once free space is above the
// configured threshold. This avoids the user having to manually retry each job
// after clearing disk space.
func (a *App) diskRecoveryWatchdog(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Trigger periodic cleanup of completed and orphaned media
			a.cleanupCompletedJobMedia()
			a.cleanupOrphanedMedia()

			disk := getDiskInfo(a.cfg.DataDir)
			minFree := a.cfg.MinFreeDiskGB
			if minFree <= 0 {
				minFree = 5.0
			}
			if disk.TotalGB == 0 || disk.FreeGB < minFree {
				// Still not enough space — don't resume yet.
				continue
			}
			// Enough space recovered: re-queue disk_full jobs.
			a.mu.RLock()
			ids := make([]string, 0)
			for _, oid := range a.order {
				j := a.jobs[oid]
				if j != nil && j.Status == "failed" &&
					classifyFailure(j.Error, j.Logs) == "disk_full" {
					ids = append(ids, j.ID)
				}
			}
			a.mu.RUnlock()
			for _, jobID := range ids {
				if _, err := a.retryJob(jobID); err != nil {
					fmt.Printf("disk recovery watchdog skipped job %s: %v\n", jobID, err)
				} else {
					fmt.Printf("disk recovery watchdog resumed disk_full job %s (free: %.1f GB)\n", jobID, disk.FreeGB)
				}
			}
		}
	}
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

func isActiveJobMedia(j *Job) bool {
	if j == nil {
		return false
	}
	if j.Status == "running" || j.Status == "queued" || j.Status == "pending" {
		return true
	}
	if j.Status == "done" {
		// Keep media until submission review has passed
		return j.ReviewState != "passed"
	}
	if j.Status == "failed" {
		// Only keep media if the job is waiting for an active scheduled retry
		if isAutoRetryableCategory(j.FailureCategory) && !j.NextRetryAt.IsZero() {
			return true
		}
	}
	return false
}

func (a *App) cleanupOrphanedMedia() int64 {
	a.mu.RLock()
	activeIDs := make(map[string]bool, len(a.jobs))
	for id, j := range a.jobs {
		if j == nil {
			continue
		}

		// Only retain directories of jobs that are active (running, pending review, or pending transient retry)
		if isActiveJobMedia(j) {
			activeIDs[id] = true
			// Check Output
			if out := outputMap(j.Output); out != nil {
				if d, ok := out["dir"].(string); ok && d != "" {
					activeIDs[filepath.Base(d)] = true
				}
				for _, f := range stringSlice(out["video_files"]) {
					activeIDs[filepath.Base(filepath.Dir(f))] = true
				}
			}
			// Check Input
			if j.Input != nil {
				if b, err := json.Marshal(j.Input); err == nil {
					var p struct {
						ResumeDir string   `json:"resume_dir"`
						File      string   `json:"file"`
						Files     []string `json:"files"`
					}
					_ = json.Unmarshal(b, &p)
					if p.ResumeDir != "" {
						activeIDs[filepath.Base(p.ResumeDir)] = true
					}
					if p.File != "" {
						activeIDs[filepath.Base(filepath.Dir(p.File))] = true
					}
					for _, f := range p.Files {
						activeIDs[filepath.Base(filepath.Dir(f))] = true
						if rel, err := filepath.Rel(a.cfg.DataDir, f); err == nil {
							parts := strings.Split(rel, string(filepath.Separator))
							if len(parts) >= 2 {
								activeIDs[parts[1]] = true
							}
						}
					}
				}
			}
		}
	}
	a.mu.RUnlock()

	var freed int64
	for _, sub := range []string{"magnet", "youtube"} {
		parent := filepath.Join(a.cfg.DataDir, sub)
		entries, err := os.ReadDir(parent)
		if err != nil {
			continue
		}
		for _, e := range entries {
			if !e.IsDir() {
				continue
			}
			folderID := e.Name()
			if !activeIDs[folderID] {
				folderPath := filepath.Join(parent, folderID)
				size := calcDirSize(folderPath)
				if err := os.RemoveAll(folderPath); err == nil {
					freed += size
				}
			}
		}
	}
	return freed
}

func (a *App) ensureSafeDisk(ctx context.Context) error {
	minFree := a.cfg.MinFreeDiskGB
	if minFree <= 0 {
		minFree = 5.0
	}

	for i := 0; i < 15; i++ {
		disk := getDiskInfo(a.cfg.DataDir)
		if disk.TotalGB == 0 || disk.FreeGB >= minFree {
			return nil
		}
		// Free space is low! Trigger cleanups to recover space
		a.cleanupCompletedJobMedia()
		a.cleanupOrphanedMedia()

		disk = getDiskInfo(a.cfg.DataDir)
		if disk.FreeGB >= minFree {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(1 * time.Second):
		}
	}

	disk := getDiskInfo(a.cfg.DataDir)
	return fmt.Errorf("磁盘空间不足 (可用空间仅 %.1f GB，安全门限 >= %.1f GB)，暂停下载以防写满磁盘", disk.FreeGB, minFree)
}

func startMemoryWatchdog() {
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			mem := getMemoryInfo()
			if mem.TotalMB > 0 && mem.AvailableMB < 80 {
				runtime.GC()
				debug.FreeOSMemory()
			}
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
	if j.Status == "done" && j.ReviewState != "" && j.ReviewState != "passed" {
		a.mu.Unlock()
		return errors.New("cannot delete job while awaiting review approval (source files protected)")
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
		// Retain finished jobs that are still pending Bilibili review approval so
		// that the review watcher can verify moderation and safely purge source media.
		if j != nil && (j.Status == "failed" || j.Status == "canceled" || (j.Status == "done" && (j.ReviewState == "" || j.ReviewState == "passed"))) {
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

// resumableJobUpload converts a failed/canceled job with completed media into an upload-only
// retry, preserving already-downloaded local video files and existing BVIDs.
func resumableJobUpload(j *Job) (uploadReq, bool) {
	if j == nil || j.Output == nil {
		return uploadReq{}, false
	}
	out := outputMap(j.Output)
	if out == nil {
		return uploadReq{}, false
	}
	dir, _ := out["dir"].(string)
	if dir != "" {
		incomplete := false
		_ = filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
			if err == nil && info != nil && !info.IsDir() && (strings.HasSuffix(info.Name(), ".aria2") || strings.HasSuffix(info.Name(), ".part") || strings.HasSuffix(info.Name(), ".ytdl")) {
				incomplete = true
				return filepath.SkipDir
			}
			return nil
		})
		if incomplete {
			return uploadReq{}, false
		}
	}

	var files []string
	var bvid string
	if upload, ok := out["upload"].(map[string]any); ok {
		files = stringSlice(upload["files"])
		if bv, ok := upload["bvid"].(string); ok && bv != "" {
			bvid = bv
		}
	}
	if len(files) == 0 {
		files = stringSlice(out["video_files"])
	}
	if len(files) == 0 && dir != "" {
		_ = filepath.Walk(dir, func(p string, info os.FileInfo, err error) error {
			if err == nil && info != nil && !info.IsDir() && isVideoFilePath(info.Name()) {
				files = append(files, p)
			}
			return nil
		})
	}
	if len(files) == 0 {
		return uploadReq{}, false
	}

	complete := make([]string, 0, len(files))
	for _, file := range uniqueMediaFiles(files) {
		st, err := os.Stat(file)
		if err == nil && st.Size() > 0 {
			complete = append(complete, file)
		}
	}
	if len(complete) == 0 {
		return uploadReq{}, false
	}
	sort.Strings(complete)

	req := uploadReq{
		Files: complete,
		Parts: len(complete) > 1,
		VID:   bvid,
	}

	if j.Kind == "pipeline" {
		var pipeline pipelineReq
		if b, err := json.Marshal(j.Input); err == nil {
			_ = json.Unmarshal(b, &pipeline)
		}
		req.Source = pipeline.URL
		req.OriginalURL = pipeline.URL
		req.Tid = pipeline.Tid
		req.Tag = pipeline.Tags
		req.Translate = pipeline.Translate
	} else if j.Kind == "youtube" {
		var y youtubeReq
		if b, err := json.Marshal(j.Input); err == nil {
			_ = json.Unmarshal(b, &y)
		}
		req.Source = y.URL
		req.OriginalURL = y.URL
		req.Tid = y.Tid
		req.Tag = y.Tags
		req.Translate = y.Translate
	} else if j.Kind == "magnet" {
		var m magnetReq
		if b, err := json.Marshal(j.Input); err == nil {
			_ = json.Unmarshal(b, &m)
		}
		req.Source = m.URL
		if req.Source == "" {
			req.Source = m.Magnet
		}
		req.OriginalURL = req.Source
		req.Tid = m.Tid
		req.Tag = m.Tags
		req.Translate = m.Translate
	} else if j.Kind == "biliup" {
		var up uploadReq
		if b, err := json.Marshal(j.Input); err == nil {
			_ = json.Unmarshal(b, &up)
		}
		req.Title = up.Title
		req.Description = up.Description
		req.Cover = up.Cover
		req.Tag = up.Tag
		req.Tid = up.Tid
		req.Source = up.Source
		req.OriginalURL = up.OriginalURL
		if req.OriginalURL == "" && (validYouTube(up.Source) || validTorrentOrMagnet(up.Source)) {
			req.OriginalURL = up.Source
		}
		req.Translate = up.Translate
		if req.VID == "" {
			req.VID = up.VID
		}
	}

	if upload, ok := out["upload"].(map[string]any); ok {
		if t, ok := upload["title"].(string); ok && t != "" {
			req.Title = t
		}
		if d, ok := upload["description"].(string); ok && d != "" {
			req.Description = d
		}
		if c, ok := upload["cover"].(string); ok && c != "" {
			req.Cover = c
		}
		if tid, ok := upload["tid"].(string); ok && tid != "" {
			req.Tid = tid
		}
		if tags, ok := upload["tags"].(string); ok && tags != "" {
			req.Tag = tags
		}
	}
	return req, true
}

func stringSlice(value any) []string {
	items, ok := value.([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(items))
	for _, item := range items {
		if s, ok := item.(string); ok && s != "" {
			out = append(out, s)
		}
	}
	return out
}

func (a *App) retryJob(jobID string) (*Job, error) {
	return a.retryJobWithCount(jobID, false)
}

func (a *App) retryJobManual(jobID string) (*Job, error) {
	return a.retryJobWithCount(jobID, true)
}

func (a *App) retryJobWithCount(jobID string, resetCount bool) (*Job, error) {
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

	retryKind := old.Kind
	retryInput := old.Input
	if old.Status == "failed" || old.Status == "canceled" {
		if req, ok := resumableJobUpload(old); ok {
			retryKind = "biliup"
			retryInput = req
		} else {
			sourceURL := ""
			tid := "188"
			tags := ""
			translate := false
			if old.Kind == "biliup" {
				if up, ok := old.Input.(uploadReq); ok {
					sourceURL = up.OriginalURL
					if sourceURL == "" {
						sourceURL = up.Source
					}
					tid = up.Tid
					tags = up.Tag
					translate = up.Translate
				} else {
					var up uploadReq
					if b, err := json.Marshal(old.Input); err == nil {
						_ = json.Unmarshal(b, &up)
						sourceURL = up.OriginalURL
						if sourceURL == "" {
							sourceURL = up.Source
						}
						tid = up.Tid
						tags = up.Tag
						translate = up.Translate
					}
				}
			} else if old.Kind == "pipeline" {
				var p pipelineReq
				if b, err := json.Marshal(old.Input); err == nil {
					_ = json.Unmarshal(b, &p)
					sourceURL = p.URL
					tid = p.Tid
					tags = p.Tags
					translate = p.Translate
				}
			} else if old.Kind == "youtube" {
				var y youtubeReq
				if b, err := json.Marshal(old.Input); err == nil {
					_ = json.Unmarshal(b, &y)
					sourceURL = y.URL
					tid = y.Tid
					tags = y.Tags
					translate = y.Translate
				}
			} else if old.Kind == "magnet" {
				var m magnetReq
				if b, err := json.Marshal(old.Input); err == nil {
					_ = json.Unmarshal(b, &m)
					sourceURL = m.URL
					if sourceURL == "" {
						sourceURL = m.Magnet
					}
					tid = m.Tid
					tags = m.Tags
					translate = m.Translate
				}
			}

			// If sourceURL is a synthetic label (e.g. magnet-recovery-8a7cef125593ba70),
			// look up related jobs by ID to resolve the actual underlying URL.
			if sourceURL != "" && !validYouTube(sourceURL) && !validTorrentOrMagnet(sourceURL) {
				targetID := strings.TrimPrefix(sourceURL, "magnet-recovery-")
				targetID = strings.TrimPrefix(targetID, "magnet-job-")
				targetID = strings.TrimPrefix(targetID, "youtube-recovery-")
				targetID = strings.TrimPrefix(targetID, "youtube-job-")
				if targetID != sourceURL {
					if related, ok := a.jobs[targetID]; ok && related != nil {
						if b, err := json.Marshal(related.Input); err == nil {
							var relatedP struct {
								URL    string `json:"url"`
								Magnet string `json:"magnet"`
								Source string `json:"source"`
							}
							_ = json.Unmarshal(b, &relatedP)
							if relatedP.URL != "" && (validYouTube(relatedP.URL) || validTorrentOrMagnet(relatedP.URL)) {
								sourceURL = relatedP.URL
							} else if relatedP.Magnet != "" && validTorrentOrMagnet(relatedP.Magnet) {
								sourceURL = relatedP.Magnet
							} else if relatedP.Source != "" && (validYouTube(relatedP.Source) || validTorrentOrMagnet(relatedP.Source)) {
								sourceURL = relatedP.Source
							}
						}
					}
				}
			}

			if sourceURL != "" && (validYouTube(sourceURL) || validTorrentOrMagnet(sourceURL)) {
				retryKind = "pipeline"
				retryInput = pipelineReq{
					URL:       sourceURL,
					Tid:       tid,
					Tags:      tags,
					Translate: translate,
				}
			} else if out := outputMap(old.Output); out != nil && old.Kind == "pipeline" {
				if dir, ok := out["dir"].(string); ok && dir != "" {
					var pipeline pipelineReq
					if b, err := json.Marshal(old.Input); err == nil {
						_ = json.Unmarshal(b, &pipeline)
					}
					pipeline.ResumeDir = dir
					retryInput = pipeline
				}
			} else if old.Kind == "biliup" {
				a.mu.Unlock()
				return nil, errors.New("cannot retry: media files are missing on disk and no valid source URL is available to re-download")
			}
		}
	}

	// A previous version created a new record for every retry. Collapse those
	// terminal duplicates before retrying, and never start a second copy when
	// the same input is already queued or running.
	oldSource := jobSourceKey(retryKind, retryInput)
	activeDuplicate := (*Job)(nil)
	removeIDs := make(map[string]bool)
	for oid, candidate := range a.jobs {
		if oid == jobID || candidate == nil || candidate.Kind != retryKind {
			continue
		}
		if jobSourceKey(candidate.Kind, candidate.Input) != oldSource {
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
		ID:             id(),
		Kind:           retryKind,
		Status:         "queued",
		Step:           "排队中",
		Created:        time.Now(),
		Input:          retryInput,
		ctx:            ctx,
		cancelFunc:     cancel,
		AutoRetryCount: func() int {
			if resetCount {
				return 0
			}
			return old.AutoRetryCount
		}(),
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

	dataDir := os.Getenv("Y2B_DATA")
	if dataDir == "" {
		dataDir = "/srv/y2b/data"
	}
	cfgDir := filepath.Join(dataDir, ".config")
	cacheDir := filepath.Join(dataDir, ".cache")
	_ = os.MkdirAll(cfgDir, 0750)
	_ = os.MkdirAll(cacheDir, 0750)
	c.Env = append(c.Env,
		"XDG_CONFIG_HOME="+cfgDir,
		"XDG_CACHE_HOME="+cacheDir,
		"HOME="+dataDir,
	)

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

const defaultBTTrackers = "udp://tracker.opentrackr.org:1337/announce,udp://open.tracker.cl:1337/announce,udp://tracker.openbittorrent.com:6969/announce,http://tracker.openbittorrent.com:80/announce,udp://opentracker.i2p.rocks:6969/announce,udp://open.demonii.com:1337/announce,udp://tracker.torrent.eu.org:451/announce,udp://explodie.org:6969/announce,udp://tracker.moeking.me:6969/announce,udp://p4p.arenabg.com:1337/announce,udp://tracker.dler.org:6969/announce,udp://bt1.archive.org:6969/announce,udp://bt2.archive.org:6969/announce,udp://tracker.theoks.net:6969/announce,udp://tracker.altrosky.nl:2710/announce,udp://movies.zsw.ca:6969/announce,http://tracker.ipv6tracker.ru:80/announce"

func buildYTDLPArgs(rawURL, quality, subLangs, cookiePath string, isPlaylist, splitChapters bool, destDir string) []string {
	langs := strings.TrimSpace(subLangs)
	if langs == "" {
		langs = "zh-Hans,zh,en,zh-Hant"
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
		args = append(args, "--write-subs", "--sub-langs", langs)
	}

	switch quality {
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
				"-o", "chapter:"+filepath.Join(destDir, "P%(playlist_index|1)02d - C%(section_number)02d. %(section_title)s.%(ext)s"),
			)
		}
		args = append(args,
			"--write-thumbnail",
			"--write-description",
			"--embed-metadata",
			"--merge-output-format", "mp4",
			"-o", filepath.Join(destDir, "P%(playlist_index|1)02d. %(title)s [%(id)s].%(ext)s"),
			rawURL,
		)
	} else {
		if splitChapters {
			args = append(args,
				"--split-chapters",
				"-o", "chapter:"+filepath.Join(destDir, "%(title)s - P%(section_number)02d. %(section_title)s.%(ext)s"),
			)
		}
		args = append(args,
			"--write-thumbnail",
			"--write-description",
			"--embed-metadata",
			"--merge-output-format", "mp4",
			"-o", filepath.Join(destDir, "%(title)s [%(id)s].%(ext)s"),
			rawURL,
		)
	}
	return args
}

func buildAria2Args(targetURL, destDir, selectFile, btPort string) []string {
	magnetArgs := []string{
		"--dir=" + destDir,
		"--continue=true",
		"--allow-overwrite=false",
		"--seed-time=0",
		"--file-allocation=none",
		"--disk-cache=16M",
		"--timeout=30",
		"--connect-timeout=15",
		"--bt-tracker-connect-timeout=15",
		"--bt-tracker-timeout=20",
		"--listen-port=" + btPort,
		"--max-connection-per-server=4",
		"--bt-max-peers=120",
		"--max-concurrent-downloads=1",
		"--enable-dht=true",
		"--enable-dht6=true",
		"--dht-entry-point=dht.transmissionbt.com:6881",
		"--dht-entry-point=router.bittorrent.com:6881",
		"--dht-entry-point=router.utorrent.com:6881",
		"--dht-entry-point=dht.aelitis.com:6881",
		"--enable-peer-exchange=true",
		"--bt-enable-lpd=true",
		"--follow-torrent=mem",
		"--bt-stop-timeout=300",
		"--summary-interval=1",
		"--bt-tracker=" + defaultBTTrackers,
		targetURL,
	}
	if strings.TrimSpace(selectFile) != "" {
		magnetArgs = append(magnetArgs, "--select-file="+strings.TrimSpace(selectFile))
	}
	return magnetArgs
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
				if err := a.ensureSafeDisk(nj.ctx); err != nil {
					downloadErr = err
					return
				}

				a.set(nj, "running", "", nil, "")
				a.setStep(nj, "下载媒体中")
				cookiePath, cleanup, _ := prepareCookies(a.cfg.Cookies, d)
				defer cleanup()

				isPlaylist := isPlaylistURL(q.URL)
				splitChapters, splitLog := a.chapterSplitDecision(nj.ctx, q.URL, cookiePath, q.SplitChapters)
				if splitLog != "" {
					totalLogs += splitLog + "\n"
				}
				args := buildYTDLPArgs(q.URL, q.Quality, q.SubLangs, cookiePath, isPlaylist, splitChapters, d)

				ytLogs, err := runCmdProgress(nj.ctx, a.cfg.YTDLP, args, func(line string) { a.progressLine(nj, "YouTube 下载", line) })
				totalLogs = ytLogs
				downloadErr = err

				files, _ := filepath.Glob(filepath.Join(d, "*"))
				sort.Strings(files)
				var chapterFiles []string
				for _, f := range files {
					name := filepath.Base(f)
					baseFiles = append(baseFiles, name)
					if isVideoFilePath(name) {
						if strings.Contains(name, " - P") || strings.Contains(name, " - C") {
							chapterFiles = append(chapterFiles, f)
						} else {
							videoFiles = append(videoFiles, f)
						}
					}
				}
				targetUploadFiles = videoFiles
				if len(chapterFiles) > 0 {
					// A playlist may contain both chapter-split videos and
					// ordinary videos. Keep both in the same multi-P submission.
					targetUploadFiles = append(videoFiles, chapterFiles...)
				}
				targetUploadFiles = uniqueMediaFiles(targetUploadFiles)

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
					return
				}
				// Partial playlist salvage: if yt-dlp exited with error but some
				// videos were already downloaded, proceed to upload those rather
				// than marking the whole job as failed. This handles the common
				// case of YouTube bot-challenge errors mid-playlist.
				category := classifyFailure(downloadErr.Error(), totalLogs)
				if q.AutoUpload && len(targetUploadFiles) > 0 && category != "unknown" {
					totalLogs += fmt.Sprintf("\n[部分下载挽救] yt-dlp 遇到错误但已下载 %d 个视频，继续上传已有部分 (分类: %s)\n", len(targetUploadFiles), category)
					downloadErr = nil // salvage: continue to upload
				} else {
					a.set(nj, "failed", downloadErr.Error(), outMap, totalLogs)
					return
				}
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

				// Keep source media until the asynchronous Bilibili review passes.
				outMap["review_state"] = "pending"
				totalLogs += "\n[审核保护] 投稿接口返回成功，源视频暂不删除，等待B站审核通过。\n"
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

func purgeVideoFilesInDir(dir string) int64 {
	if strings.TrimSpace(dir) == "" {
		return 0
	}
	var videos []string
	_ = filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info == nil || info.IsDir() {
			return nil
		}
		if isVideoFilePath(info.Name()) {
			videos = append(videos, path)
		}
		return nil
	})
	return purgeVideoFiles(videos...)
}

func (a *App) cleanupCompletedJobMedia() int64 {
	a.mu.RLock()
	jobs := make([]*Job, 0, len(a.jobs))
	for _, j := range a.jobs {
		if j != nil && j.Status == "done" && j.ReviewState == "passed" {
			copy := *j
			jobs = append(jobs, &copy)
		}
	}
	a.mu.RUnlock()
	var freed int64
	root := filepath.Clean(a.cfg.DataDir)
	for _, j := range jobs {
		var out struct {
			Dir string `json:"dir"`
		}
		if b, err := json.Marshal(j.Output); err == nil {
			_ = json.Unmarshal(b, &out)
		}
		if out.Dir != "" {
			freed += purgeVideoFilesInDir(out.Dir)
			cleanDir := filepath.Clean(out.Dir)
			if a.cfg.DataDir != "" {
				if rel, err := filepath.Rel(root, cleanDir); err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) && !filepath.IsAbs(rel) {
					_ = os.RemoveAll(cleanDir)
				}
			}
		}
		if a.cfg.DataDir != "" && j.ID != "" {
			for _, sub := range []string{"magnet", "youtube"} {
				jobDir := filepath.Join(a.cfg.DataDir, sub, j.ID)
				if fi, err := os.Stat(jobDir); err == nil && fi.IsDir() {
					_ = os.RemoveAll(jobDir)
				}
			}
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

// Biliup exposes the moderation result through `biliup show <BV>`. Keep the
// parser independent from the CLI's tracing logs so it also works with older
// biliup versions that print logs before the JSON document.
type biliReviewVideo struct {
	Index        int    `json:"index"`
	Title        string `json:"title"`
	XcodeState   int    `json:"xcode_state"` // 6=done, <0=transcoding/encoding failed
	FailCode     int    `json:"fail_code"`
	FailDesc     string `json:"fail_desc"` // e.g. "视频编码错误", "转码失败"
	Status       int    `json:"status"`    // 0=ok, 2=reject/failed
	StatusDesc   string `json:"status_desc"`
	RejectReason string `json:"reject_reason"`
	ModifyAdvice string `json:"modify_advise"`
	ProblemDesc  string `json:"problem_description"`
}

type biliReviewResult struct {
	Archive struct {
		State        int    `json:"state"`
		StateDesc    string `json:"state_desc"`
		HadPassed    bool   `json:"had_passed"`
		RejectReason string `json:"reject_reason"`
		ProblemDesc  string `json:"problem_description"`
		ModifyAdvice string `json:"modify_advise"`
	} `json:"archive"`
	Videos []biliReviewVideo `json:"videos"`
}

func parseBiliReviewOutput(raw string) (biliReviewResult, error) {
	var result biliReviewResult
	raw = strings.ToValidUTF8(raw, "")
	start := strings.IndexByte(raw, '{')
	if start < 0 {
		return result, errors.New("biliup show 未返回审核 JSON")
	}
	jsonStr := raw[start:]
	if end := strings.LastIndexByte(jsonStr, '}'); end >= 0 {
		jsonStr = jsonStr[:end+1]
	}
	if err := json.Unmarshal([]byte(jsonStr), &result); err != nil {
		return result, fmt.Errorf("解析B站审核结果失败: %w", err)
	}
	return result, nil
}

func biliReviewSummary(result biliReviewResult) (state string, reason string) {
	// 1. Check video-level transcoding & encoding errors
	var failParts []string
	for _, v := range result.Videos {
		partNum := v.Index
		if partNum <= 0 {
			partNum = 1
		}
		if v.XcodeState < 0 || v.FailCode != 0 || v.FailDesc != "" || strings.Contains(v.StatusDesc, "转码失败") || strings.Contains(v.StatusDesc, "编码错误") || strings.Contains(v.StatusDesc, "失败") {
			errMsg := v.FailDesc
			if errMsg == "" {
				errMsg = v.StatusDesc
			}
			if errMsg == "" {
				errMsg = fmt.Sprintf("转码/编码失败 (xcode_state: %d, fail_code: %d)", v.XcodeState, v.FailCode)
			}
			failParts = append(failParts, fmt.Sprintf("P%d: %s", partNum, errMsg))
		} else if v.RejectReason != "" {
			failParts = append(failParts, fmt.Sprintf("P%d: %s", partNum, v.RejectReason))
		}
	}

	if len(failParts) > 0 {
		return "rejected", strings.Join(failParts, "; ")
	}

	// 2. Check archive-level review state
	desc := strings.TrimSpace(result.Archive.StateDesc)
	switch {
	case strings.Contains(desc, "退回"), strings.Contains(desc, "不通过"), strings.Contains(desc, "驳回"), strings.Contains(desc, "锁定"):
		rejectMsg := result.Archive.RejectReason
		if rejectMsg == "" {
			rejectMsg = result.Archive.ProblemDesc
		}
		if rejectMsg == "" {
			rejectMsg = desc
		}
		return "rejected", rejectMsg
	case result.Archive.HadPassed,
		strings.Contains(desc, "通过"), strings.Contains(desc, "已发布"), strings.Contains(desc, "开放浏览"):
		return "passed", ""
	default:
		return "pending", ""
	}
}

func biliReviewState(result biliReviewResult) string {
	state, _ := biliReviewSummary(result)
	return state
}

func (a *App) fetchBiliReview(ctx context.Context, bvid string) (biliReviewResult, string, error) {
	args := []string{"-u", a.cfg.BiliCookies, "show", bvid}
	logs, err := runCmdProgress(ctx, a.cfg.Biliup, args, nil)
	if err != nil {
		return biliReviewResult{}, logs, fmt.Errorf("查询B站审核失败: %w", err)
	}
	result, parseErr := parseBiliReviewOutput(logs)
	return result, logs, parseErr
}

type reviewViolation struct {
	Part  int
	Whole bool
	Start float64
	End   float64
}

func parseReviewTimestamp(s string) float64 {
	parts := strings.Split(s, ":")
	if len(parts) != 2 && len(parts) != 3 {
		return -1
	}
	var values []float64
	for _, part := range parts {
		v, err := strconv.ParseFloat(strings.TrimSpace(part), 64)
		if err != nil {
			return -1
		}
		values = append(values, v)
	}
	if len(values) == 2 {
		return values[0]*60 + values[1]
	}
	return values[0]*3600 + values[1]*60 + values[2]
}

func parseReviewViolations(reason string) []reviewViolation {
	re := regexp.MustCompile(`(?i)P(\d+)(?:\(?(\d{1,2}:\d{2}(?::\d{2})?)-(\d{1,2}:\d{2}(?::\d{2})?)\)?|内容全程)`)
	matches := re.FindAllStringSubmatch(reason, -1)
	violations := make([]reviewViolation, 0, len(matches))
	for _, match := range matches {
		part, err := strconv.Atoi(match[1])
		if err != nil || part < 1 {
			continue
		}
		if strings.Contains(match[0], "内容全程") {
			violations = append(violations, reviewViolation{Part: part, Whole: true})
			continue
		}
		start, end := parseReviewTimestamp(match[2]), parseReviewTimestamp(match[3])
		if start >= 0 && end > start {
			violations = append(violations, reviewViolation{Part: part, Start: start, End: end})
		}
	}
	return violations
}

func asStringSlice(value any) []string {
	items, ok := value.([]any)
	if !ok {
		return nil
	}
	result := make([]string, 0, len(items))
	for _, item := range items {
		if s, ok := item.(string); ok && s != "" {
			result = append(result, s)
		}
	}
	return result
}

func (a *App) reviewUploadFiles(ctx context.Context, files []string, reason, repairDir string) ([]string, string, error) {
	violations := parseReviewViolations(reason)
	if len(violations) == 0 {
		return nil, "", errors.New("审核退回原因未包含可自动修复的分P或时间点")
	}
	byPart := make(map[int][]reviewViolation)
	for _, violation := range violations {
		byPart[violation.Part] = append(byPart[violation.Part], violation)
	}
	if err := os.MkdirAll(repairDir, 0750); err != nil {
		return nil, "", err
	}
	result := make([]string, 0, len(files))
	var logs strings.Builder
	for index, file := range files {
		part := index + 1
		violationsForPart := byPart[part]
		if len(violationsForPart) == 0 {
			name := safePartFilename(partTitleFromFile(file), index, filepath.Ext(file))
			target := filepath.Join(repairDir, name)
			if err := os.Symlink(file, target); err != nil && !os.IsExist(err) {
				return nil, logs.String(), err
			}
			result = append(result, target)
			continue
		}
		whole := false
		for _, violation := range violationsForPart {
			whole = whole || violation.Whole
		}
		if whole {
			logs.WriteString(fmt.Sprintf("[审核修复] 删除 P%d 全部内容\n", part))
			continue
		}
		target := filepath.Join(repairDir, safePartFilename(partTitleFromFile(file)+"（审核修复）", index, filepath.Ext(file)))
		if err := trimReviewVideo(ctx, file, target, violationsForPart); err != nil {
			return nil, logs.String(), fmt.Errorf("修复 P%d 失败: %w", part, err)
		}
		logs.WriteString(fmt.Sprintf("[审核修复] P%d 已移除 %d 个违规时间段\n", part, len(violationsForPart)))
		result = append(result, target)
	}
	if len(result) == 0 {
		return nil, logs.String(), errors.New("审核修复后没有可投稿的视频")
	}
	return result, logs.String(), nil
}

func trimReviewVideo(ctx context.Context, input, output string, violations []reviewViolation) error {
	durationRaw, err := runCmdProgress(ctx, "ffprobe", []string{"-v", "error", "-show_entries", "format=duration", "-of", "default=nw=1:nk=1", input}, nil)
	if err != nil {
		return err
	}
	duration, err := strconv.ParseFloat(strings.TrimSpace(durationRaw), 64)
	if err != nil || duration <= 0 {
		return fmt.Errorf("无法读取视频时长")
	}
	sort.Slice(violations, func(i, j int) bool { return violations[i].Start < violations[j].Start })
	type segment struct{ start, end float64 }
	segments := make([]segment, 0, len(violations)+1)
	cursor := 0.0
	for _, violation := range violations {
		start, end := math.Max(0, violation.Start), math.Min(duration, violation.End)
		if start > cursor {
			segments = append(segments, segment{cursor, start})
		}
		if end > cursor {
			cursor = end
		}
	}
	if cursor < duration {
		segments = append(segments, segment{cursor, duration})
	}
	if len(segments) == 0 {
		return errors.New("违规时间段覆盖了整个视频")
	}
	filters := make([]string, 0, len(segments)*2)
	concatInputs := make([]string, 0, len(segments)*2)
	for i, segment := range segments {
		filters = append(filters,
			fmt.Sprintf("[0:v]trim=start=%f:end=%f,setpts=PTS-STARTPTS[v%d]", segment.start, segment.end, i),
			fmt.Sprintf("[0:a]atrim=start=%f:end=%f,asetpts=PTS-STARTPTS[a%d]", segment.start, segment.end, i))
		concatInputs = append(concatInputs, fmt.Sprintf("[v%d][a%d]", i, i))
	}
	filters = append(filters, strings.Join(concatInputs, "")+fmt.Sprintf("concat=n=%d:v=1:a=1[v][a]", len(segments)))
	args := []string{"-y", "-i", input, "-filter_complex", strings.Join(filters, ";"), "-map", "[v]", "-map", "[a]", "-c:v", "libx264", "-preset", "veryfast", "-crf", "18", "-c:a", "aac", "-movflags", "+faststart", output}
	_, err = runCmdProgress(ctx, "ffmpeg", args, nil)
	return err
}

func outputMap(value any) map[string]any {
	b, err := json.Marshal(value)
	if err != nil {
		return nil
	}
	var result map[string]any
	if json.Unmarshal(b, &result) != nil {
		return nil
	}
	return result
}

func (a *App) reviewUploadRequest(j *Job, files []string, previous map[string]any) uploadReq {
	q := uploadReq{Files: files, Parts: len(files) > 1}
	if j.Kind == "pipeline" {
		var p pipelineReq
		if b, err := json.Marshal(j.Input); err == nil {
			_ = json.Unmarshal(b, &p)
		}
		q.Translate, q.Tid, q.Tag, q.Source = p.Translate, p.Tid, p.Tags, p.URL
	} else if b, err := json.Marshal(j.Input); err == nil {
		_ = json.Unmarshal(b, &q)
	}
	if previous != nil {
		if value, ok := previous["title"].(string); ok {
			q.Title = value
		}
		if value, ok := previous["description"].(string); ok {
			q.Description = value
		}
		if value, ok := previous["tags"].(string); ok {
			q.Tag = value
		}
		if value, ok := previous["tid"].(string); ok {
			q.Tid = value
		}
	}
	return q
}

func (a *App) reviewJob(ctx context.Context, jobID string) {
	a.mu.RLock()
	original := a.jobs[jobID]
	if original == nil || original.Status != "done" || original.ReviewState == "passed" {
		a.mu.RUnlock()
		return
	}
	job := *original
	a.mu.RUnlock()

	out := outputMap(job.Output)
	if out == nil {
		return
	}

	// Handle streamed magnet upload case where out["upload"] is a slice of maps
	if uploadList, isList := out["upload"].([]any); isList && len(uploadList) > 0 {
		allPassed := true
		now := time.Now()
		var reasons []string
		for _, item := range uploadList {
			if itemMap, isMap := item.(map[string]any); isMap {
				if bvid, _ := itemMap["bvid"].(string); bvid != "" {
					result, _, err := a.fetchBiliReview(ctx, bvid)
					if err == nil {
						st, reason := biliReviewSummary(result)
						itemMap["review_state"] = st
						if st != "passed" {
							allPassed = false
							if reason != "" {
								reasons = append(reasons, reason)
							}
						}
					} else {
						allPassed = false
					}
				}
			}
		}
		a.mu.Lock()
		if current := a.jobs[jobID]; current != nil {
			current.ReviewCheckedAt = now
			if allPassed {
				current.ReviewState = "passed"
				current.ReviewError = ""
				if dir, ok := out["dir"].(string); ok && dir != "" {
					purgeVideoFilesInDir(dir)
				}
			} else if len(reasons) > 0 {
				current.ReviewState = "rejected"
				current.ReviewError = strings.Join(reasons, "; ")
			}
			current.Output = out
		}
		a.mu.Unlock()
		a.saveJobs()
		return
	}

	upload, ok := out["upload"].(map[string]any)
	if !ok {
		// Direct /api/biliup/upload jobs store the upload result at the
		// top-level instead of under pipeline.output.upload.
		if _, hasBVID := out["bvid"].(string); !hasBVID {
			return
		}
		upload = out
	}
	bvid, _ := upload["bvid"].(string)
	if bvid == "" {
		return
	}
	result, showLogs, err := a.fetchBiliReview(ctx, bvid)
	now := time.Now()
	if err != nil {
		a.mu.Lock()
		if current := a.jobs[jobID]; current != nil {
			current.ReviewState = "pending"
			current.ReviewError = err.Error()
			current.ReviewCheckedAt = now
		}
		a.mu.Unlock()
		a.saveJobs()
		return
	}
	state, reason := biliReviewSummary(result)
	a.mu.Lock()
	if current := a.jobs[jobID]; current != nil {
		current.ReviewState = state
		current.ReviewError = reason
		current.ReviewCheckedAt = now
	}
	a.mu.Unlock()
	a.saveJobs()

	if state == "pending" {
		return
	}
	if state == "passed" {
		files := asStringSlice(upload["files"])
		if dir, ok := out["dir"].(string); ok && dir != "" {
			purgeVideoFilesInDir(dir)
		} else {
			a.purgeManagedVideoFiles(files...)
		}
		if repairDir, ok := upload["review_repair_dir"].(string); ok && repairDir != "" {
			purgeVideoFilesInDir(repairDir)
		}
		upload["review_state"] = "passed"
		a.mu.Lock()
		if current := a.jobs[jobID]; current != nil {
			current.Output = out
			current.ReviewError = ""
		}
		a.mu.Unlock()
		a.saveJobs()
		return
	}

	if job.ReviewRepairs >= a.cfg.ReviewRepairMax {
		a.mu.Lock()
		if current := a.jobs[jobID]; current != nil {
			current.ReviewState = "rejected"
			current.ReviewError = "审核退回，已达到自动修复次数上限"
		}
		a.mu.Unlock()
		a.saveJobs()
		return
	}

	files := asStringSlice(upload["files"])
	if len(files) == 0 {
		a.mu.Lock()
		if current := a.jobs[jobID]; current != nil {
			current.ReviewState = "rejected"
			current.ReviewError = "审核退回但找不到原始视频文件，无法自动修复"
		}
		a.mu.Unlock()
		a.saveJobs()
		return
	}
	repairReason := result.Archive.RejectReason + " " + result.Archive.ProblemDesc + " " + result.Archive.ModifyAdvice
	repairDir := filepath.Join(filepath.Dir(files[0]), ".y2b-review-repair-"+bvid+"-"+strconv.FormatInt(now.Unix(), 10))
	repaired, repairLogs, repairErr := a.reviewUploadFiles(ctx, files, repairReason, repairDir)
	if repairErr != nil {
		a.mu.Lock()
		if current := a.jobs[jobID]; current != nil {
			current.ReviewState = "rejected"
			current.ReviewError = repairErr.Error()
			current.Logs += "\n[审核查询]\n" + showLogs + "\n[审核修复]\n" + repairLogs
		}
		a.mu.Unlock()
		a.saveJobs()
		return
	}

	if cap := a.uploadSlots; cap != nil {
		if slotErr := a.acquireSlot(ctx, cap); slotErr != nil {
			return
		}
		defer func() { <-cap }()
	}
	newUpload, uploadLogs, uploadErr := a.executeBiliupUpload(ctx, a.reviewUploadRequest(&job, repaired, upload))
	a.mu.Lock()
	if current := a.jobs[jobID]; current != nil {
		current.ReviewRepairs++
		current.ReviewCheckedAt = now
		current.ReviewState = "pending"
		current.ReviewError = ""
		current.Logs += "\n[审核查询]\n" + showLogs + "\n[审核修复]\n" + repairLogs + "\n[自动重新投稿]\n" + uploadLogs
		if uploadErr == nil {
			newUpload["previous_bvid"] = bvid
			newUpload["review_state"] = "pending"
			newUpload["review_repair_dir"] = repairDir
			current.Output = out
			current.Output.(map[string]any)["upload"] = newUpload
		} else {
			current.ReviewState = "rejected"
			current.ReviewError = uploadErr.Error()
		}
	}
	a.mu.Unlock()
	a.saveJobs()
}

func (a *App) startReviewWatcher(ctx context.Context) {
	interval := a.cfg.ReviewInterval
	if interval <= 0 {
		interval = 10 * time.Minute
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			a.reviewMu.Lock()
			a.mu.RLock()
			ids := make([]string, 0, len(a.order))
			for _, id := range a.order {
				if j := a.jobs[id]; j != nil && j.Status == "done" && j.ReviewState != "passed" {
					ids = append(ids, id)
				}
			}
			a.mu.RUnlock()
			for _, id := range ids {
				a.reviewJob(ctx, id)
			}
			a.reviewMu.Unlock()
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
}

// Magnet Downloader
type magnetReq struct {
	Magnet     string `json:"magnet"`
	URL        string `json:"url"`
	SelectFile string `json:"select_file"` // aria2 torrent file index/range, e.g. "3" or "3,7-9"
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

func validAriaSelectFile(s string) bool {
	s = strings.TrimSpace(s)
	return s == "" || regexp.MustCompile(`^[0-9]+([,-][0-9]+)*$`).MatchString(s)
}

func isSingleAriaSelectFile(s string) bool {
	return regexp.MustCompile(`^[0-9]+$`).MatchString(strings.TrimSpace(s))
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

	var destDir string
	for _, arg := range args {
		if strings.HasPrefix(arg, "--dir=") {
			destDir = strings.TrimPrefix(arg, "--dir=")
			break
		}
	}

	magnetCtx, cancel := context.WithCancel(ctx)
	if a.cfg.MagnetTimeout > 0 {
		magnetCtx, cancel = context.WithTimeout(ctx, a.cfg.MagnetTimeout)
	}
	defer cancel()

	var quotaErr error
	var quotaMu sync.Mutex
	if destDir != "" && (a.cfg.MaxJobDiskGB > 0 || a.cfg.MinFreeDiskGB > 0) {
		stopMon := make(chan struct{})
		defer close(stopMon)
		go func() {
			ticker := time.NewTicker(3 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-stopMon:
					return
				case <-magnetCtx.Done():
					return
				case <-ticker.C:
					if a.cfg.MaxJobDiskGB > 0 {
						size := calcDirSize(destDir)
						gb := float64(size) / (1024 * 1024 * 1024)
						if gb > a.cfg.MaxJobDiskGB {
							quotaMu.Lock()
							quotaErr = fmt.Errorf("quota_exceeded: 任务下载体积 (%.1f GB) 超过单任务安全配额上限 (%.1f GB)，已中止下载以防写满磁盘", gb, a.cfg.MaxJobDiskGB)
							quotaMu.Unlock()
							cancel()
							return
						}
					}
					if a.cfg.MinFreeDiskGB > 0 {
						disk := getDiskInfo(a.cfg.DataDir)
						if disk.TotalGB > 0 && disk.FreeGB < (a.cfg.MinFreeDiskGB/2) {
							quotaMu.Lock()
							quotaErr = fmt.Errorf("磁盘空间不足 (可用空间仅 %.1f GB，安全门限 >= %.1f GB)，暂停下载以防写满磁盘", disk.FreeGB, a.cfg.MinFreeDiskGB)
							quotaMu.Unlock()
							cancel()
							return
						}
					}
				}
			}
		}()
	}

	logs, err := runCmdProgress(magnetCtx, a.cfg.Aria2, args, onLine)
	quotaMu.Lock()
	if quotaErr != nil {
		err = quotaErr
	}
	quotaMu.Unlock()
	if err == nil {
		return logs, nil
	}
	if quotaErr != nil {
		return logs, quotaErr
	}
	if errors.Is(magnetCtx.Err(), context.DeadlineExceeded) {
		return logs, fmt.Errorf("magnet_timeout: BT 下载超过 %s", a.cfg.MagnetTimeout)
	}
	if isDeadSeedOutput(logs) {
		return logs, fmt.Errorf("dead_seed: 未发现可用做种或下载速度持续为 0")
	}
	return logs, err
}

// runMagnetStreamingUpload keeps aria2 downloading while completed video files
// are uploaded one at a time. aria2 removes the .aria2 control file only after
// a file is complete, so this is safe for large multi-file torrents. A single
// huge video still has to finish before an uploader can read it reliably.
func (a *App) runMagnetStreamingUpload(ctx context.Context, args []string, dir string, onLine func(string), upload func(string) error) (string, []string, error) {
	streamCtx, cancel := context.WithCancel(ctx)
	if a.cfg.MagnetTimeout > 0 {
		streamCtx, cancel = context.WithTimeout(ctx, a.cfg.MagnetTimeout)
	}
	defer cancel()

	c := exec.CommandContext(streamCtx, a.cfg.Aria2, args...)
	c.Env = os.Environ()
	capture := &progressCapture{buffer: &limitedBuffer{max: 128 << 10}, onLine: onLine}
	c.Stdout = capture
	c.Stderr = capture
	if err := c.Start(); err != nil {
		return "", nil, err
	}

	done := make(chan error, 1)
	go func() { done <- c.Wait() }()

	type fileState struct {
		size int64
		seen int
	}
	states := make(map[string]fileState)
	uploaded := make(map[string]bool)
	var uploadedFiles []string
	uploadReady := func(force bool) error {
		var firstErr error
		_ = filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
			if err != nil || info == nil || info.IsDir() || uploaded[path] || !isVideoFilePath(path) {
				return nil
			}
			if info.Size() == 0 {
				return nil
			}
			// The direct sidecar is present while aria2 is still writing this file.
			if _, err := os.Stat(path + ".aria2"); err == nil {
				return nil
			}
			// Verify media container integrity (ensure moov atom and complete headers)
			if !validateMediaIntegrity(path) {
				return nil
			}

			st := states[path]
			if st.size == info.Size() {
				st.seen++
			} else {
				st.size, st.seen = info.Size(), 1
			}
			states[path] = st
			if !force && st.seen < 2 { // protect against a sidecar disappearing just before a final write
				return nil
			}
			if err := upload(path); err != nil {
				firstErr = err
				return filepath.SkipAll
			}
			uploaded[path] = true
			uploadedFiles = append(uploadedFiles, path)
			return nil
		})
		return firstErr
	}

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	var processErr error
	for {
		select {
		case processErr = <-done:
			// One final scan catches files completed immediately before aria2 exits.
			if err := uploadReady(true); err != nil {
				_ = c.Process.Kill()
				return strings.TrimSpace(capture.String()), uploadedFiles, err
			}
			capture.flush()
			logs := strings.TrimSpace(capture.String())
			if processErr != nil {
				if errors.Is(streamCtx.Err(), context.DeadlineExceeded) {
					return logs, uploadedFiles, fmt.Errorf("magnet_timeout: BT 下载超过 %s", a.cfg.MagnetTimeout)
				}
				if isDeadSeedOutput(logs) {
					return logs, uploadedFiles, fmt.Errorf("dead_seed: 未发现可用做种或下载速度持续为 0")
				}
				return logs, uploadedFiles, fmt.Errorf("%s: %w: %s", a.cfg.Aria2, processErr, logs)
			}
			return logs, uploadedFiles, nil
		case <-ticker.C:
			if a.cfg.MaxJobDiskGB > 0 {
				size := calcDirSize(dir)
				gb := float64(size) / (1024 * 1024 * 1024)
				if gb > a.cfg.MaxJobDiskGB {
					_ = c.Process.Kill()
					return strings.TrimSpace(capture.String()), uploadedFiles, fmt.Errorf("quota_exceeded: 任务下载体积 (%.1f GB) 超过单任务安全配额上限 (%.1f GB)，已中止下载以防写满磁盘", gb, a.cfg.MaxJobDiskGB)
				}
			}
			if a.cfg.MinFreeDiskGB > 0 {
				disk := getDiskInfo(a.cfg.DataDir)
				if disk.TotalGB > 0 && disk.FreeGB < (a.cfg.MinFreeDiskGB/2) {
					_ = c.Process.Kill()
					return strings.TrimSpace(capture.String()), uploadedFiles, fmt.Errorf("磁盘空间不足 (可用空间仅 %.1f GB，安全门限 >= %.1f GB)，暂停下载以防写满磁盘", disk.FreeGB, a.cfg.MinFreeDiskGB)
				}
			}
			if err := uploadReady(false); err != nil {
				_ = c.Process.Kill()
				return strings.TrimSpace(capture.String()), uploadedFiles, err
			}
		case <-ctx.Done():
			_ = c.Process.Kill()
			return strings.TrimSpace(capture.String()), uploadedFiles, ctx.Err()
		}
	}
}

// runYouTubeStreamingUpload executes yt-dlp while periodically checking for completed
// individual video files and triggering onVideoReady as each item finishes.
func (a *App) runYouTubeStreamingUpload(ctx context.Context, args []string, dir string, onLine func(string), onVideoReady func(file string) error) (string, []string, error) {
	streamCtx, cancel := context.WithCancel(ctx)
	if a.cfg.DownloadTimeout > 0 {
		streamCtx, cancel = context.WithTimeout(ctx, a.cfg.DownloadTimeout)
	}
	defer cancel()

	c := exec.CommandContext(streamCtx, a.cfg.YTDLP, args...)
	c.Env = os.Environ()
	capture := &progressCapture{buffer: &limitedBuffer{max: 128 << 10}, onLine: onLine}
	c.Stdout = capture
	c.Stderr = capture
	if err := c.Start(); err != nil {
		return "", nil, err
	}

	done := make(chan error, 1)
	go func() { done <- c.Wait() }()

	type fileState struct {
		size int64
		seen int
	}
	states := make(map[string]fileState)
	uploaded := make(map[string]bool)
	var uploadedFiles []string

	hasSplitChapters := false
	for _, a := range args {
		if a == "--split-chapters" {
			hasSplitChapters = true
			break
		}
	}

	scanReady := func(force bool) error {
		var firstErr error
		files, _ := filepath.Glob(filepath.Join(dir, "*"))
		sort.Strings(files)

		for _, path := range files {
			if uploaded[path] {
				continue
			}
			// When running in real-time streaming mode, ignore standalone audio streams (e.g. .m4a)
			// as yt-dlp downloads them separately before merging them into .mp4 containers.
			// Only allow pure audio files if force=true (after yt-dlp exits) AND no video containers exist.
			if !force && isPureAudioFilePath(path) {
				continue
			}
			if !isVideoFilePath(path) {
				continue
			}
			// If chapter splitting is active, skip parent un-split video files during active streaming,
			// because yt-dlp will postprocess them into individual chapters (e.g. - C01) and delete the parent.
			if !force && hasSplitChapters {
				baseName := filepath.Base(path)
				if !strings.Contains(baseName, " - C") && !strings.Contains(baseName, " - P") {
					continue
				}
			}
			// Skip if yt-dlp temporary .part or .ytdl file exists
			if _, err := os.Stat(path + ".part"); err == nil {
				continue
			}
			if _, err := os.Stat(path + ".ytdl"); err == nil {
				continue
			}
			base := strings.TrimSuffix(path, filepath.Ext(path))
			if _, err := os.Stat(base + ".m4a.part"); err == nil {
				continue
			}
			if _, err := os.Stat(base + ".webm.part"); err == nil {
				continue
			}
			if _, err := os.Stat(base + ".mp4.part"); err == nil {
				continue
			}
			if _, err := os.Stat(base + ".ytdl"); err == nil {
				continue
			}
			// Verify media container integrity (ensure moov atom and complete headers)
			if !validateMediaIntegrity(path) {
				continue
			}
			info, err := os.Stat(path)
			if err != nil || info.IsDir() || info.Size() == 0 {
				continue
			}

			st := states[path]
			if st.size == info.Size() {
				st.seen++
			} else {
				st.size, st.seen = info.Size(), 1
			}
			states[path] = st

			if !force && st.seen < 2 {
				continue
			}

			// Pre-upload subtitle preparation
			convertVttToSrtAndBcc(dir)

			if err := onVideoReady(path); err != nil {
				firstErr = err
				break
			}
			uploaded[path] = true
			uploadedFiles = append(uploadedFiles, path)
		}
		return firstErr
	}

	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case processErr := <-done:
			if err := scanReady(true); err != nil {
				_ = c.Process.Kill()
				return strings.TrimSpace(capture.String()), uploadedFiles, err
			}
			capture.flush()
			logs := strings.TrimSpace(capture.String())
			if processErr != nil {
				if errors.Is(streamCtx.Err(), context.DeadlineExceeded) {
					return logs, uploadedFiles, fmt.Errorf("download_timeout: YouTube 下载超过 %s", a.cfg.DownloadTimeout)
				}
				return logs, uploadedFiles, fmt.Errorf("%s: %w: %s", a.cfg.YTDLP, processErr, logs)
			}
			return logs, uploadedFiles, nil

		case <-ticker.C:
			if a.cfg.MinFreeDiskGB > 0 {
				disk := getDiskInfo(a.cfg.DataDir)
				if disk.TotalGB > 0 && disk.FreeGB < (a.cfg.MinFreeDiskGB/2) {
					_ = c.Process.Kill()
					return strings.TrimSpace(capture.String()), uploadedFiles, fmt.Errorf("磁盘空间不足 (可用空间仅 %.1f GB，安全门限 >= %.1f GB)，暂停下载以防写满磁盘", disk.FreeGB, a.cfg.MinFreeDiskGB)
				}
			}
			if err := scanReady(false); err != nil {
				_ = c.Process.Kill()
				return strings.TrimSpace(capture.String()), uploadedFiles, err
			}

		case <-ctx.Done():
			_ = c.Process.Kill()
			return strings.TrimSpace(capture.String()), uploadedFiles, ctx.Err()
		}
	}
}

// streamUploader handles streaming concurrent uploads for individual files
// as they become ready during downloads.
type streamUploader struct {
	app           *App
	job           *Job
	q             pipelineReq
	mainBVID      string
	mainTitle     string
	uploadedFiles []string
	results       []map[string]any
	totalBytes    int64
	mu            sync.Mutex
}

func (a *App) newStreamUploader(nj *Job, q pipelineReq) *streamUploader {
	return &streamUploader{
		app: a,
		job: nj,
		q:   q,
	}
}

func (s *streamUploader) handleReadyVideo(file string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	nj := s.job
	a := s.app

	if err := a.acquireSlot(nj.ctx, a.uploadSlots); err != nil {
		return err
	}
	defer func() { <-a.uploadSlots }()

	partNum := len(s.uploadedFiles) + 1
	a.setStep(nj, fmt.Sprintf("[2/2] B站边下边传 (第 %d 集): %s", partNum, filepath.Base(file)))
	a.setProgress(nj, JobProgress{Detail: fmt.Sprintf("B站上传 P%d: %s", partNum, filepath.Base(file))})

	uploadFile := file
	if s.q.BurnSubs {
		burned, _, _ := burnSubtitlesToVideos(nj.ctx, filepath.Dir(file), []string{file}, func(line string) {
			a.progressLine(nj, fmt.Sprintf("字幕压制 P%d", partNum), line)
		})
		if len(burned) > 0 {
			uploadFile = burned[0]
		}
	}

	upReq := uploadReq{
		File:      uploadFile,
		Translate: s.q.Translate,
		Tid:       s.q.Tid,
		Tag:       s.q.Tags,
		Source:    s.q.URL,
		Parts:     false,
		Progress:  func(line string) { a.progressLine(nj, fmt.Sprintf("B站上传 P%d", partNum), line) },
	}

	if s.mainBVID != "" {
		upReq.VID = s.mainBVID
	} else if s.mainTitle != "" {
		upReq.Title = s.mainTitle
	}

	result, _, err := a.executeBiliupUpload(nj.ctx, upReq)
	if err != nil {
		return err
	}

	if s.mainBVID == "" {
		if bv, ok := result["bvid"].(string); ok && bv != "" {
			s.mainBVID = bv
		}
		if t, ok := result["title"].(string); ok && t != "" {
			s.mainTitle = t
			a.mu.Lock()
			if nj.Title == "" {
				nj.Title = t
			}
			a.mu.Unlock()
		}
	}

	s.uploadedFiles = append(s.uploadedFiles, uploadFile)
	s.results = append(s.results, result)

	fileSize := calcFilesSize([]string{uploadFile})
	if fileSize > 0 {
		s.totalBytes += fileSize
		a.recordDownload(fileSize)
		a.recordUpload(fileSize)
	}

	// Dynamic disk space reclamation: Only under critical disk pressure (< MinFreeDiskGB / 4),
	// remove the uploaded local video file to prevent disk exhaustion during massive torrents.
	// Under normal conditions, preserve local files until B站 review passes so auto-repair can work.
	disk := getDiskInfo(a.cfg.DataDir)
	if a.cfg.MinFreeDiskGB > 0 && disk.TotalGB > 0 && disk.FreeGB < (a.cfg.MinFreeDiskGB/4) {
		_ = os.Remove(uploadFile)
		base := strings.TrimSuffix(uploadFile, filepath.Ext(uploadFile))
		_ = os.Remove(base + ".srt")
		_ = os.Remove(base + ".vtt")
		_ = os.Remove(base + ".bcc")
	}

	return nil
}

func isPureAudioFilePath(path string) bool {
	switch strings.ToLower(filepath.Ext(path)) {
	case ".m4a", ".aac", ".wav", ".flac", ".ogg", ".opus", ".mp3":
		return true
	default:
		return false
	}
}

func isVideoContainerFilePath(path string) bool {
	return isVideoFilePath(path) && !isPureAudioFilePath(path)
}

func isVideoFilePath(path string) bool {
	switch strings.ToLower(filepath.Ext(path)) {
	case ".mp4", ".mkv", ".avi", ".webm", ".mp3", ".m4v", ".mov",
		".flv", ".ts", ".mts", ".m2ts", ".3gp", ".ogv", ".f4v",
		".mpg", ".mpeg", ".wmv", ".asf", ".rm", ".rmvb", ".mxf",
		".m4a", ".aac", ".wav", ".flac", ".ogg", ".opus":
		return true
	default:
		return false
	}
}

// validateMediaIntegrity performs fast, non-destructive container validation to ensure
// a video file is structurally sound and complete before submission.
func validateMediaIntegrity(path string) bool {
	info, err := os.Stat(path)
	if err != nil || info.IsDir() || info.Size() < 16 {
		return false
	}

	ext := strings.ToLower(filepath.Ext(path))
	switch ext {
	case ".mp4", ".m4v", ".mov", ".m4a":
		return validateMP4Integrity(path, info.Size())
	case ".mkv", ".webm":
		return validateMKVIntegrity(path, info.Size())
	case ".avi":
		return validateAVIIntegrity(path, info.Size())
	default:
		return info.Size() > 16
	}
}

// validateMP4Integrity scans top-level ISO base media file format boxes (atoms).
// A valid, decodable MP4/MOV must contain 'moov' (or 'moof') atom.
func validateMP4Integrity(path string, size int64) bool {
	f, err := os.Open(path)
	if err != nil {
		return false
	}
	defer f.Close()

	var offset int64
	hasFtyp := false
	hasMoovOrMoof := false
	buf := make([]byte, 8)

	for offset < size {
		n, err := f.ReadAt(buf, offset)
		if err != nil || n < 8 {
			break
		}
		boxSize := int64(binary.BigEndian.Uint32(buf[0:4]))
		boxType := string(buf[4:8])

		if boxType == "ftyp" {
			hasFtyp = true
		} else if boxType == "moov" || boxType == "moof" {
			hasMoovOrMoof = true
		}

		if boxSize == 0 {
			// Box extends to end of file
			break
		}
		if boxSize == 1 {
			// 64-bit extended size
			extBuf := make([]byte, 8)
			if n, err := f.ReadAt(extBuf, offset+8); err != nil || n < 8 {
				break
			}
			boxSize = int64(binary.BigEndian.Uint64(extBuf))
			if boxSize < 16 {
				break
			}
		} else if boxSize < 8 {
			// Invalid box size
			break
		}

		offset += boxSize
	}

	return hasFtyp && hasMoovOrMoof
}

func validateMKVIntegrity(path string, size int64) bool {
	f, err := os.Open(path)
	if err != nil {
		return false
	}
	defer f.Close()

	header := make([]byte, 4)
	if n, err := f.Read(header); err != nil || n < 4 {
		return false
	}
	// EBML header: 0x1A 0x45 0xDF 0xA3
	return bytes.Equal(header, []byte{0x1A, 0x45, 0xDF, 0xA3})
}

func validateAVIIntegrity(path string, size int64) bool {
	f, err := os.Open(path)
	if err != nil {
		return false
	}
	defer f.Close()

	header := make([]byte, 12)
	if n, err := f.Read(header); err != nil || n < 12 {
		return false
	}
	// RIFF....AVI
	return string(header[0:4]) == "RIFF" && string(header[8:12]) == "AVI "
}

// uniqueMediaFiles prevents the same path being counted/uploaded twice when
// a derived list is combined with its source list or when a directory walk
// encounters a path through multiple branches.
func uniqueMediaFiles(files []string) []string {
	out := make([]string, 0, len(files))
	seen := make(map[string]struct{}, len(files))
	for _, file := range files {
		if strings.TrimSpace(file) == "" || !isVideoFilePath(file) {
			continue
		}
		key := filepath.Clean(file)
		if abs, err := filepath.Abs(key); err == nil {
			key = abs
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, file)
	}
	return out
}

func (a *App) magnet(w http.ResponseWriter, r *http.Request) {
	var q magnetReq
	if decode(r, &q) != nil {
		jsonResp(w, 400, map[string]string{"error": "JSON required"})
		return
	}
	if !validAriaSelectFile(q.SelectFile) {
		jsonResp(w, 400, map[string]string{"error": "select_file must be aria2 file indexes, e.g. 3 or 3,7-9"})
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
			var streamedUploads []map[string]any

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
				if err := a.ensureSafeDisk(nj.ctx); err != nil {
					downloadErr = err
					return
				}

				a.set(nj, "running", "", nil, "")
				a.setStep(nj, "磁力下载中")
				magnetArgs := buildAria2Args(m, d, q.SelectFile, a.cfg.BTListenPort)
				var magLogs string
				var err error
				// A streamed upload has no single review record for the whole
				// manuscript. Keep auto-upload in the reviewed batch path so every
				// source file remains available until its submission is approved.
				if q.AutoUpload {
					uploader := a.newStreamUploader(nj, pipelineReq{
						URL:       m,
						Translate: q.Translate,
						Tid:       q.Tid,
						Tags:      q.Tags,
					})
					magLogs, uploadedFiles, err := a.runMagnetStreamingUpload(nj.ctx, magnetArgs, d, func(line string) { a.progressLine(nj, "BT 下载", line) }, uploader.handleReadyVideo)
					totalLogs += magLogs
					downloadErr = err
					videoFiles = uniqueMediaFiles(uploadedFiles)
					streamedUploads = uploader.results
					if len(uploadedFiles) > 0 {
						videoFile = uploadedFiles[0]
					}
				} else {
					magLogs, err = a.runMagnetProgress(nj.ctx, magnetArgs, func(line string) { a.progressLine(nj, "BT 下载", line) })
					totalLogs += magLogs
					downloadErr = err
				}

				_ = filepath.Walk(d, func(p string, info os.FileInfo, err error) error {
					if err != nil || info.IsDir() {
						return nil
					}
					name := info.Name()
					baseFiles = append(baseFiles, name)
					ext := strings.ToLower(filepath.Ext(name))
					if isVideoFilePath(name) {
						videoFiles = append(videoFiles, p)
						if videoFile == "" || ext == ".mp4" {
							videoFile = p
						}
					}
					return nil
				})
				videoFiles = uniqueMediaFiles(videoFiles)
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

			// Streaming uploads have already been committed and cleaned as each
			// file completed. Do not run the old second upload pass.
			if q.AutoUpload && len(streamedUploads) > 0 {
				if len(streamedUploads) == 0 {
					a.set(nj, "failed", "no video files found after download", outMap, totalLogs)
					return
				}
				a.recordPipelineSuccess()
				if q.Translate {
					a.recordAiTrans()
				}
				outMap["upload"] = streamedUploads
				outMap["stream_upload"] = true
				outMap["review_state"] = "pending"
				totalLogs += "\n[审核保护] 投稿接口返回成功，源视频暂不删除，等待B站审核通过。\n"
				a.set(nj, "done", "", outMap, totalLogs)
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

				freed := purgeVideoFilesInDir(d)
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
	OriginalURL string       `json:"original_url,omitempty"`
	Translate   bool         `json:"translate"`
	Parts       bool         `json:"parts"`
	VID         string       `json:"vid,omitempty"`
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
	case -101, -400, -663, 21016, 21017, 21018, 21070, 21071:
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
	case 406, 601, 21564, 21085:
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
	reCode := regexp.MustCompile(`code["']?\s*:\s*["']?(-?\d+)`)
	if m := reCode.FindStringSubmatch(logs); len(m) > 1 {
		if c, err := strconv.Atoi(m[1]); err == nil {
			res.Code = c
		}
	}
	reMsg := regexp.MustCompile(`message["']?\s*:\s*["']([^"']+)["']`)
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
		var translatedTitle string
		if a.cfg.DeepSeekKey != "" {
			res, err := a.callLLMEnhance(ctx, partTitle, "")
			if err == nil && res != nil && res.Title != "" {
				translatedTitle = sanitizeBiliTitle(res.Title)
			}
		}
		if translatedTitle == "" {
			tCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			zh, err := freeTranslateCtx(tCtx, partTitle, "zh-CN")
			cancel()
			if err == nil && zh != "" {
				translatedTitle = sanitizeBiliTitle(zh)
			}
		}
		if translatedTitle == "" || translatedTitle == partTitle {
			continue
		}
		if fi, err := os.Stat(file); err != nil || fi.IsDir() || fi.Size() == 0 {
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
		absFile, absErr := filepath.Abs(file)
		if absErr == nil {
			file = absFile
		}
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
	if err := a.waitUploadCooldown(ctx); err != nil {
		return nil, "", err
	}
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

	const maxBiliParts = 100
	if len(files) > maxBiliParts && q.VID == "" {
		var combinedLogs strings.Builder
		bvids := make([]string, 0)
		uploadResults := make([]map[string]any, 0)
		totalChunks := (len(files) + maxBiliParts - 1) / maxBiliParts

		for chunkIdx := 0; chunkIdx < totalChunks; chunkIdx++ {
			start := chunkIdx * maxBiliParts
			end := start + maxBiliParts
			if end > len(files) {
				end = len(files)
			}
			chunkFiles := files[start:end]

			chunkReq := q
			chunkReq.Files = chunkFiles
			chunkReq.File = chunkFiles[0]
			chunkTitle := fmt.Sprintf("%s 第 %d 部分", title, chunkIdx+1)
			chunkReq.Title = chunkTitle

			if q.Progress != nil {
				q.Progress(fmt.Sprintf("[多卷投稿] 正在处理第 %d/%d 卷 (%d 个分P)...", chunkIdx+1, totalChunks, len(chunkFiles)))
			}

			chunkOut, chunkLog, chunkErr := a.executeSingleBiliupUpload(ctx, chunkReq, chunkTitle, desc, tags, tid)
			combinedLogs.WriteString(fmt.Sprintf("\n=== 第 %d/%d 部分投稿日志 ===\n%s\n", chunkIdx+1, totalChunks, chunkLog))
			if chunkErr != nil {
				return nil, combinedLogs.String(), fmt.Errorf("第 %d/%d 卷投稿失败: %w", chunkIdx+1, totalChunks, chunkErr)
			}
			if bv, ok := chunkOut["bvid"].(string); ok && bv != "" {
				bvids = append(bvids, bv)
			}
			uploadResults = append(uploadResults, chunkOut)
		}

		firstBVID := ""
		if len(bvids) > 0 {
			firstBVID = bvids[0]
		}
		res := map[string]any{
			"title":        title,
			"description":  desc,
			"tags":         tags,
			"tid":          tid,
			"files":        files,
			"bvid":         firstBVID,
			"bvids":        bvids,
			"bili_url":     "https://www.bilibili.com/video/" + firstBVID,
			"volumes":      uploadResults,
			"total_parts":  len(files),
			"volume_count": totalChunks,
		}
		return res, combinedLogs.String(), nil
	}

	return a.executeSingleBiliupUpload(ctx, q, title, desc, tags, tid)
}

func (a *App) executeSingleBiliupUpload(ctx context.Context, q uploadReq, title, desc, tags, tid string) (map[string]any, string, error) {
	files := q.Files
	if len(files) == 0 && q.File != "" {
		files = []string{q.File}
	}
	uploadFiles := files
	cleanupTranslated := func() {}
	translatedCount := 0
	partTranslateLogs := ""
	if q.Translate {
		uploadFiles, cleanupTranslated, translatedCount, partTranslateLogs = a.prepareTranslatedPartFiles(ctx, files)
		defer cleanupTranslated()
	}

	cover := q.Cover
	if cover == "" {
		cover = adjacentCover(ctx, files[0])
	}
	if q.Progress != nil {
		q.Progress("[ffmpeg] 正在优化投稿封面")
	}
	cover = a.sanitizeBiliCover(ctx, cover, q.Progress)

	// Multi-endpoint submission with smart auto-healing
	// Prioritize b-cut-android and app to minimize web 406 rate limiting
	submitEndpoints := []string{"b-cut-android", "app", "web"}
	if a.cfg.SubmitEndpoint != "" && a.cfg.SubmitEndpoint != "auto" {
		submitEndpoints = []string{a.cfg.SubmitEndpoint}
	}
	var totalLogs string
	var execErr error
	var bvid string
	var subtitleLogs string

	attempt := 0
	useCover := (cover != "")

	for _, ep := range submitEndpoints {
		attempt++
		var args []string
		if q.VID != "" {
			args = append([]string{"--user-cookie", a.cfg.BiliCookies, "append", "--vid", q.VID}, uploadFiles...)
		} else {
			args = append([]string{"--user-cookie", a.cfg.BiliCookies, "upload"}, uploadFiles...)
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
			if bvid == "" && q.VID != "" {
				bvid = q.VID
			}
			execErr = nil
			break
		}

		// 2. Automated Code Analysis & Self-Healing Decision
		switch biliRepairActionFor(res.Code) {
		case biliRepairStop:
			if res.Code == 21070 || res.Code == 21071 {
				execErr = fmt.Errorf("B站提示：检测到重复稿件或相同视频正在审核中 (code %d: %s)", res.Code, res.Message)
			} else if res.Code == -663 {
				execErr = fmt.Errorf("B站登录凭证鉴权失败 (code %d: %s)，请在控制台更新 cookies.json", res.Code, res.Message)
			} else if res.Code == -400 {
				execErr = fmt.Errorf("B站请求错误/凭证无效 (code %d: %s)，请检查 cookies.json 或视频参数", res.Code, res.Message)
			} else {
				execErr = fmt.Errorf("B站登录凭证失效 (code %d: %s)，请在控制台更新 cookies.json", res.Code, res.Message)
			}
			goto finish

		case biliRepairRateLimit:
			totalLogs += fmt.Sprintf("[风控/限流保护] B站提示限流或需人工验证 (code %d: %s)。这不是错误，停止快速切换线路，本地视频已完整保留，任务将自动挂起等待人工在B站完成验证或次日自动刷新重试。\n", res.Code, res.Message)
			execErr = fmt.Errorf("B站投稿限流/需验证 (code %d: %s)，任务已挂起等待人工验证或次日刷新", res.Code, res.Message)
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

		select {
		case <-ctx.Done():
			execErr = ctx.Err()
			goto finish
		case <-time.After(2 * time.Second):
		}
	}

finish:
	// Repair branches continue with the next endpoint. If all bounded attempts
	// are exhausted without a BVID, preserve the failure instead of returning
	// a false success with a nil error.
	if bvid == "" && q.VID != "" {
		bvid = q.VID
	}
	if bvid == "" && execErr == nil {
		execErr = fmt.Errorf("B站投稿失败：已尝试 %d 个提交通道，自动修复后仍未成功", attempt)
	}
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
		if logs, err := a.uploadYouTubeSubtitles(ctx, bvid, filepath.Dir(files[0])); err != nil {
			subtitleLogs = fmt.Sprintf("[字幕上传失败] %v\n%s", err, logs)
		} else if logs != "" {
			subtitleLogs = "[字幕上传]\n" + logs
		}
		totalLogs += "\n" + subtitleLogs
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
	if subtitleLogs != "" {
		res["subtitle_upload"] = subtitleLogs
	}
	if translatedCount > 0 {
		totalLogs = partTranslateLogs + totalLogs
		res["translated_parts"] = translatedCount
	}
	return res, totalLogs, execErr
}


func (a *App) waitUploadCooldown(ctx context.Context) error {
	for {
		a.mu.RLock()
		wait := time.Until(a.uploadCooldownUntil)
		a.mu.RUnlock()
		if wait <= 0 {
			return nil
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
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
				logs += "\n[审核保护] 投稿接口返回成功，源视频暂不删除，等待B站审核通过。\n"
			}
			return out, logs, err
		})
	}
}

// One-Click End-to-End Pipeline
type pipelineReq struct {
	URL           string `json:"url"` // YouTube or Magnet
	ResumeDir     string `json:"resume_dir,omitempty"`
	SelectFile    string `json:"select_file"` // aria2 torrent file index/range for Magnet URLs
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
	if !validAriaSelectFile(q.SelectFile) {
		jsonResp(w, 400, map[string]string{"error": "select_file must be aria2 file indexes, e.g. 3 or 3,7-9"})
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

			uploader := a.newStreamUploader(nj, q)

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
				if err := a.ensureSafeDisk(nj.ctx); err != nil {
					downloadErr = err
					return
				}

				a.set(nj, "running", "", nil, "")
				if isYT {
					a.setStep(nj, "[1/2] YouTube 视频解析与下载")
					d := q.ResumeDir
					if d == "" {
						d = filepath.Join(a.cfg.DataDir, "youtube", nj.ID)
					}
					_ = os.MkdirAll(d, 0750)
					targetDir = d
					cookiePath, cleanup, _ := prepareCookies(a.cfg.Cookies, d)
					defer cleanup()

					isPlaylist := isPlaylistURL(q.URL)
					splitChapters, splitLog := a.chapterSplitDecision(nj.ctx, q.URL, cookiePath, q.SplitChapters)
					if splitLog != "" {
						totalLogs += splitLog + "\n"
					}
					args := buildYTDLPArgs(q.URL, q.Quality, q.SubLangs, cookiePath, isPlaylist, splitChapters, d)

					if isPlaylist || splitChapters {
						a.setStep(nj, "[1/2] YouTube 边下载边投稿")
						ytLogs, uploadedFiles, err := a.runYouTubeStreamingUpload(nj.ctx, args, d, func(line string) { a.progressLine(nj, "YouTube 下载", line) }, uploader.handleReadyVideo)
						totalLogs += "[YouTube Streaming Download Logs]\n" + ytLogs + "\n"
						downloadErr = err
						targetUploadFiles = uniqueMediaFiles(uploadedFiles)
					} else {
						ytLogs, err := runCmdProgress(nj.ctx, a.cfg.YTDLP, args, func(line string) { a.progressLine(nj, "YouTube 下载", line) })
						totalLogs += "[YouTube Download Logs]\n" + ytLogs + "\n"
						downloadErr = err
						if downloadErr != nil && !errors.Is(downloadErr, context.Canceled) {
							// Don't bail immediately; scan for any files already downloaded.
						} else if downloadErr != nil {
							return
						}

						files, _ := filepath.Glob(filepath.Join(d, "*"))
						sort.Strings(files)
						var videoFiles []string
						for _, f := range files {
							name := filepath.Base(f)
							if isVideoFilePath(name) {
								videoFiles = append(videoFiles, f)
							}
						}
						targetUploadFiles = uniqueMediaFiles(videoFiles)

						convertVttToSrtAndBcc(d)
						if q.BurnSubs && len(targetUploadFiles) > 0 {
							a.setStep(nj, "[1/2] 正在压制中英硬字幕...")
							a.setProgress(nj, JobProgress{Detail: "ffmpeg 字幕压制"})
							burned, bLogs, _ := burnSubtitlesToVideos(nj.ctx, d, targetUploadFiles, func(line string) { a.progressLine(nj, "ffmpeg 字幕压制", line) })
							targetUploadFiles = burned
							totalLogs += "\n[字幕压制日志]\n" + bLogs
						}
					}
				} else {
					a.setStep(nj, "[1/2] 磁力边下载边投稿")
					d := q.ResumeDir
					if d == "" {
						d = filepath.Join(a.cfg.DataDir, "magnet", nj.ID)
					}
					_ = os.MkdirAll(d, 0750)
					targetDir = d
					magnetArgs := buildAria2Args(q.URL, d, q.SelectFile, a.cfg.BTListenPort)
					magLogs, uploadedFiles, err := a.runMagnetStreamingUpload(nj.ctx, magnetArgs, d, func(line string) { a.progressLine(nj, "BT 下载", line) }, uploader.handleReadyVideo)
					totalLogs += "[Magnet Download Logs]\n" + magLogs + "\n"
					downloadErr = err
					if downloadErr != nil && len(uploadedFiles) == 0 {
						return
					}

					files, _ := filepath.Glob(filepath.Join(d, "*"))
					sort.Strings(files)
					var scannedFiles []string
					_ = filepath.Walk(d, func(p string, info os.FileInfo, err error) error {
						if err != nil || info.IsDir() {
							return nil
						}
						if isVideoFilePath(info.Name()) {
							scannedFiles = append(scannedFiles, p)
						}
						return nil
					})
					targetUploadFiles = uniqueMediaFiles(append(uploadedFiles, scannedFiles...))
					sort.Strings(targetUploadFiles)
				}
			}()

			if downloadErr != nil {
				if errors.Is(downloadErr, context.Canceled) {
					a.set(nj, "canceled", "已取消", map[string]any{"dir": targetDir}, totalLogs)
					return
				}
				category := classifyFailure(downloadErr.Error(), totalLogs)
				if (isYT || isMag) && len(uploader.results) > 0 {
					totalLogs += fmt.Sprintf("\n[部分完成挽救] 下载遇到错误但已成功流式投稿 %d 个分P，保留已投稿成果 (分类: %s)\n", len(uploader.results), category)
					downloadErr = nil
				} else if isYT && len(targetUploadFiles) > 0 && category != "unknown" {
					totalLogs += fmt.Sprintf("\n[部分下载挽救] yt-dlp 遇到错误但已下载 %d 个视频文件，继续上传已有部分 (分类: %s)\n", len(targetUploadFiles), category)
					downloadErr = nil
				} else {
					a.set(nj, "failed", downloadErr.Error(), map[string]any{"dir": targetDir, "video_files": targetUploadFiles}, totalLogs)
					return
				}
			}

			if len(uploader.results) > 0 {
				mainVideoFile = ""
				if len(uploader.uploadedFiles) > 0 {
					mainVideoFile = uploader.uploadedFiles[0]
				}
				a.recordPipelineSuccess()
				if q.Translate {
					a.recordAiTrans()
				}
				isMultiP := len(uploader.uploadedFiles) > 1
				uploadSummary := map[string]any{
					"bvid":        uploader.mainBVID,
					"title":       uploader.mainTitle,
					"bili_url":    "https://www.bilibili.com/video/" + uploader.mainBVID,
					"parts":       uploader.results,
					"total_parts": len(uploader.uploadedFiles),
				}
				a.set(nj, "done", "", map[string]any{
					"dir":           targetDir,
					"video_file":    mainVideoFile,
					"video_files":   uploader.uploadedFiles,
					"is_multi_p":    isMultiP,
					"upload":        uploadSummary,
					"stream_upload": true,
					"review_state":  "pending",
				}, totalLogs+"\n[审核保护] 流式边下边传投稿成功，源视频暂不删除，等待B站审核通过。\n")
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

				a.setStep(nj, "[2/2] B站自动化投稿")
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

			// Keep source media until the asynchronous Bilibili review passes.
			totalLogs += "\n[审核保护] 投稿接口返回成功，源视频暂不删除，等待B站审核通过。\n"

			a.set(nj, "done", "", map[string]any{
				"dir":          targetDir,
				"video_file":   mainVideoFile,
				"video_files":  targetUploadFiles,
				"is_multi_p":   len(targetUploadFiles) > 1,
				"upload":       uploadOut,
				"review_state": "pending",
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
	return freeTranslateCtx(context.Background(), text, targetLang)
}

func freeTranslateCtx(ctx context.Context, text, targetLang string) (string, error) {
	text = strings.TrimSpace(text)
	if text == "" {
		return "", nil
	}
	urlStr := fmt.Sprintf("https://translate.googleapis.com/translate_a/single?client=gtx&sl=auto&tl=%s&dt=t&q=%s",
		targetLang, url.QueryEscape(text))
	req, err := http.NewRequestWithContext(ctx, "GET", urlStr, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0")
	client := &http.Client{Timeout: 3 * time.Second}
	res, err := client.Do(req)
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
	seen := make(map[string]struct{}, len(files))
	for _, f := range files {
		key := filepath.Clean(f)
		if abs, err := filepath.Abs(key); err == nil {
			key = abs
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
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
	a.mediaCacheMu.Lock()
	if time.Since(a.mediaCache.cachedAt) < 3*time.Second && a.mediaCache.pkgs != nil {
		cached := a.mediaCache.pkgs
		a.mediaCacheMu.Unlock()
		return cached
	}
	a.mediaCacheMu.Unlock()

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
				if isVideoFilePath(name) {
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

	a.mediaCacheMu.Lock()
	a.mediaCache = mediaScanCache{
		cachedAt: time.Now(),
		pkgs:     pkgs,
	}
	a.mediaCacheMu.Unlock()
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
				if isVideoFilePath(info.Name()) {
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

	freedBytes += a.cleanupOrphanedMedia()

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
	root, _ := filepath.Abs(a.cfg.DataDir)
	target, _ := filepath.Abs(q.Folder)
	if target == root || !strings.HasPrefix(target, root+string(os.PathSeparator)) {
		jsonResp(w, 403, map[string]string{"error": "forbidden path outside data dir"})
		return
	}
	if err := os.RemoveAll(target); err != nil {
		jsonResp(w, 500, map[string]string{"error": err.Error()})
		return
	}
	jsonResp(w, 200, map[string]any{"ok": true, "folder": target})
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
	var list []*MonitoredChannel
	if err != nil || json.Unmarshal(data, &list) != nil {
		if backup, backupErr := os.ReadFile(a.channelsFilePath() + ".bak"); backupErr == nil {
			_ = json.Unmarshal(backup, &list)
		}
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
	_ = writeAtomic(a.channelsFilePath(), data, 0640)
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
		// Perform initial check on startup so channels don't wait a full interval after restart
		a.checkAllChannels(ctx)
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
	if err == nil && json.Unmarshal(b, &a.stats) == nil {
		return
	}
	if backup, backupErr := os.ReadFile(a.statsFile() + ".bak"); backupErr == nil && json.Unmarshal(backup, &a.stats) == nil {
		return
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
		_ = writeAtomic(a.statsFile(), b, 0640)
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
		"ok":                true,
		"version":           Version,
		"build_commit":      BuildCommit,
		"build_time":        BuildTime,
		"server_started_at": serverStartTime.Format(time.RFC3339),
		"uptime_seconds":    int(time.Since(serverStartTime).Seconds()),
		"time":              time.Now().Format(time.RFC3339),
		"data_dir":          a.cfg.DataDir,
		"total_jobs":        totalJobs,
		"running_jobs":  runningJobs,
		"ram":           ram,
		"rom":           rom,
		"disk_free_gb":     rom.FreeGB,
		"disk_total_gb":    rom.TotalGB,
		"min_free_disk_gb": a.cfg.MinFreeDiskGB,
		"max_job_disk_gb":  a.cfg.MaxJobDiskGB,
		"disk_used_pct": func() float64 {
			if rom.TotalGB > 0 {
				return (rom.TotalGB - rom.FreeGB) / rom.TotalGB * 100
			}
			return 0
		}(),
		"disk_warning": rom.TotalGB > 0 && rom.FreeGB < a.cfg.MinFreeDiskGB,
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

type flexibleBool bool

func (b *flexibleBool) UnmarshalJSON(data []byte) error {
	dataStr := strings.Trim(strings.TrimSpace(string(data)), "\"")
	switch strings.ToLower(dataStr) {
	case "true", "1":
		*b = true
		return nil
	case "false", "0", "null", "":
		*b = false
		return nil
	default:
		var num float64
		if err := json.Unmarshal(data, &num); err == nil {
			*b = (num != 0)
			return nil
		}
		*b = false
		return nil
	}
}

func (b flexibleBool) MarshalJSON() ([]byte, error) {
	if b {
		return []byte("true"), nil
	}
	return []byte("false"), nil
}

type cookieJSON struct {
	Domain         string       `json:"domain"`
	Path           string       `json:"path"`
	Name           string       `json:"name"`
	Value          string       `json:"value"`
	Expires        int64        `json:"expires"`
	ExpirationDate float64      `json:"expirationDate"`
	HTTPOnly       flexibleBool `json:"httpOnly"`
	HTTPOnlySnake  flexibleBool `json:"http_only"`
	Secure         flexibleBool `json:"secure"`
}

type cookieEnvelope struct {
	Cookies    []cookieJSON `json:"cookies"`
	CookieInfo struct {
		Cookies []cookieJSON `json:"cookies"`
	} `json:"cookie_info"`
}

func prepareCookies(src, dir string) (string, func(), error) {
	candidates := []string{src}
	if src != "" {
		cookieDir := filepath.Dir(src)
		for _, name := range []string{"youtube_cookies.txt", "youtube_cookies.json", "cookies.txt", "yt_cookies.txt", "yt_cookies.json"} {
			cand := filepath.Join(cookieDir, name)
			if cand != src {
				candidates = append(candidates, cand)
			}
		}
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
			if bool(c.HTTPOnly) || bool(c.HTTPOnlySnake) {
				domain = "#HttpOnly_" + domain
			}
			fmt.Fprintf(tmp, "%s\t%s\t%s\t%s\t%d\t%s\t%s\n", domain, "TRUE", path, map[bool]string{true: "TRUE", false: "FALSE"}[bool(c.Secure)], exp, c.Name, strings.ReplaceAll(strings.ReplaceAll(c.Value, "\t", ""), "\n", ""))
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

type biliSubtitleVideoResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    struct {
		Videos []struct {
			CID int64 `json:"cid"`
			AID int64 `json:"aid"`
		} `json:"videos"`
	} `json:"data"`
}

type biliSubtitleSaveResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// uploadYouTubeSubtitles submits the sidecar SRT files after the video has a
// BVID. Biliup's open_subtitle flag only enables the feature; it does not
// upload subtitle text by itself.
func (a *App) uploadYouTubeSubtitles(ctx context.Context, bvid, dir string) (string, error) {
	if bvid == "" || dir == "" {
		return "", nil
	}
	paths, _ := filepath.Glob(filepath.Join(dir, "*.srt"))
	if len(paths) == 0 {
		return "未找到保留的 SRT 字幕文件\n", nil
	}
	cookies, csrf, err := loadBiliCookieHeader(a.cfg.BiliCookies)
	if err != nil {
		return "", err
	}

	infoURL := "https://member.bilibili.com/x/vupre/web/archive/view?bvid=" + url.QueryEscape(bvid)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, infoURL, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Cookie", cookies)
	req.Header.Set("User-Agent", "Mozilla/5.0")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("获取 BVID/CID 失败: %w", err)
	}
	body, readErr := io.ReadAll(resp.Body)
	resp.Body.Close()
	if readErr != nil {
		return "", readErr
	}
	if resp.StatusCode/100 != 2 {
		return "", fmt.Errorf("获取 BVID/CID 返回 HTTP %d", resp.StatusCode)
	}
	var info biliSubtitleVideoResponse
	if err := json.Unmarshal(body, &info); err != nil {
		return "", fmt.Errorf("解析 BVID/CID 失败: %w", err)
	}
	if info.Code != 0 || len(info.Data.Videos) == 0 {
		return "", fmt.Errorf("获取 BVID/CID 失败: code=%d %s", info.Code, info.Message)
	}

	// A split-chapter YouTube download has one source subtitle timeline. Bind
	// it to the first uploaded part rather than falsely attaching the same
	// unshifted timeline to every chapter.
	cid := info.Data.Videos[0].CID
	aid := info.Data.Videos[0].AID
	var logs []string
	seen := map[string]bool{}
	for _, path := range paths {
		lang := subtitleLanguageFromPath(path)
		if seen[lang] {
			continue
		}
		seen[lang] = true
		bcc, err := srtFileToBCC(path)
		if err != nil {
			logs = append(logs, filepath.Base(path)+": "+err.Error())
			continue
		}
		data, _ := json.Marshal(bcc)
		form := url.Values{}
		form.Set("lan", lang)
		form.Set("submit", "true")
		form.Set("csrf", csrf)
		form.Set("sign", "false")
		form.Set("bvid", bvid)
		form.Set("type", "1")
		form.Set("oid", strconv.FormatInt(cid, 10))
		form.Set("data", string(data))
		saveReq, err := http.NewRequestWithContext(ctx, http.MethodPost,
			"https://api.bilibili.com/x/v2/dm/subtitle/draft/save", strings.NewReader(form.Encode()))
		if err != nil {
			return strings.Join(logs, "\n"), err
		}
		saveReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		saveReq.Header.Set("Cookie", cookies)
		saveReq.Header.Set("User-Agent", "Mozilla/5.0")
		saveReq.Header.Set("Origin", "https://account.bilibili.com")
		saveReq.Header.Set("Referer", fmt.Sprintf("https://account.bilibili.com/subtitle/edit/#/editor?bvid=%s&cid=%d", bvid, cid))
		saveResp, err := http.DefaultClient.Do(saveReq)
		if err != nil {
			logs = append(logs, filepath.Base(path)+": "+err.Error())
			continue
		}
		saveBody, readErr := io.ReadAll(saveResp.Body)
		saveResp.Body.Close()
		if readErr != nil {
			logs = append(logs, filepath.Base(path)+": "+readErr.Error())
			continue
		}
		var result biliSubtitleSaveResponse
		if err := json.Unmarshal(saveBody, &result); err != nil || result.Code != 0 {
			logs = append(logs, fmt.Sprintf("%s: code=%d %s", filepath.Base(path), result.Code, result.Message))
			continue
		}
		logs = append(logs, fmt.Sprintf("%s -> %s (aid=%d cid=%d)", filepath.Base(path), lang, aid, cid))
	}
	return strings.Join(logs, "\n") + "\n", nil
}

func loadBiliCookieHeader(path string) (string, string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", "", err
	}
	var env cookieEnvelope
	if err := json.Unmarshal(b, &env); err != nil {
		return "", "", fmt.Errorf("解析 B 站 cookies 失败: %w", err)
	}
	cs := env.Cookies
	if len(cs) == 0 {
		cs = env.CookieInfo.Cookies
	}
	var parts []string
	csrf := ""
	for _, c := range cs {
		if c.Name == "" || c.Value == "" {
			continue
		}
		parts = append(parts, c.Name+"="+c.Value)
		if c.Name == "bili_jct" {
			csrf = c.Value
		}
	}
	if len(parts) == 0 || csrf == "" {
		return "", "", errors.New("B 站 cookies 缺少登录态或 bili_jct")
	}
	return strings.Join(parts, "; "), csrf, nil
}

func subtitleLanguageFromPath(path string) string {
	base := strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))
	parts := strings.Split(base, ".")
	lang := "zh"
	if len(parts) > 1 {
		candidate := parts[len(parts)-1]
		if candidate != "" && regexp.MustCompile(`^[A-Za-z]{2,3}(?:-[A-Za-z]{2,4})?$`).MatchString(candidate) {
			lang = candidate
		}
	}
	switch strings.ToLower(lang) {
	case "zh-hans", "zh-cn", "cmn-hans":
		return "zh"
	case "zh-hant", "zh-tw", "cmn-hant":
		return "zh-TW"
	case "en-us":
		return "en-US"
	default:
		return strings.ToLower(lang)
	}
}

func srtFileToBCC(path string) (bccHeader, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return bccHeader{}, err
	}
	b = bytes.TrimPrefix(b, []byte("\xef\xbb\xbf"))
	content := strings.ToValidUTF8(string(b), "")
	reTime := regexp.MustCompile(`(\d{1,2}:\d{2}:\d{2}[\.,]\d{3}|\d{2}:\d{2}[\.,]\d{3})\s*-->\s*(\d{1,2}:\d{2}:\d{2}[\.,]\d{3}|\d{2}:\d{2}[\.,]\d{3})`)
	lines := strings.Split(strings.ReplaceAll(content, "\r\n", "\n"), "\n")
	var body []bccItem
	for i := 0; i < len(lines); i++ {
		line := strings.TrimSpace(lines[i])
		if !reTime.MatchString(line) {
			continue
		}
		m := reTime.FindStringSubmatch(line)
		var text []string
		for j := i + 1; j < len(lines) && strings.TrimSpace(lines[j]) != ""; j++ {
			text = append(text, strings.TrimSpace(lines[j]))
			i = j
		}
		if len(text) > 0 {
			body = append(body, bccItem{From: parseTimeToSeconds(m[1]), To: parseTimeToSeconds(m[2]), Location: 2, Content: strings.Join(text, " ")})
		}
	}
	if len(body) == 0 {
		return bccHeader{}, errors.New("字幕内容为空或 SRT 格式无效")
	}
	return bccHeader{FontSize: 0.4, FontColor: "#FFFFFF", BackgroundAlpha: 0.5, BackgroundColor: "#9C27B0", Stroke: "none", Body: body}, nil
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
		b = bytes.TrimPrefix(b, []byte("\xef\xbb\xbf"))
		content := strings.ToValidUTF8(string(b), "")
		lines := strings.Split(strings.ReplaceAll(content, "\r\n", "\n"), "\n")
		var srtLines []string
		var bccItems []bccItem
		idx := 1

		i := 0
		for i < len(lines) {
			line := strings.TrimSpace(lines[i])
			if m := reTime.FindStringSubmatch(line); len(m) == 3 {
				fromSec := parseTimeToSeconds(m[1])
				toSec := parseTimeToSeconds(m[2])

				var textLines []string
				j := i + 1
				for j < len(lines) {
					tLine := strings.TrimSpace(lines[j])
					if tLine == "" || reTime.MatchString(tLine) {
						break
					}
					cleanText := reTag.ReplaceAllString(tLine, "")
					cleanText = strings.TrimSpace(cleanText)
					if cleanText != "" {
						textLines = append(textLines, cleanText)
					}
					j++
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
				i = j
			} else {
				i++
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

func findMatchingSubtitle(dir, videoFile string) string {
	base := strings.TrimSuffix(videoFile, filepath.Ext(videoFile))
	// 1. Try video-specific subtitles first (for multi-P or split chapters)
	for _, ext := range []string{".zh-Hans.srt", ".zh.srt", ".zh-Hans.vtt", ".zh.vtt", ".en.srt", ".en.vtt", ".srt", ".vtt"} {
		p := base + ext
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}
	// 2. Try directory-wide patterns as fallback for single video
	for _, pattern := range []string{"*zh-Hans*.srt", "*zh*.srt", "*zh-Hans*.vtt", "*zh*.vtt", "*en*.srt", "*en*.vtt", "*.srt", "*.vtt"} {
		matches, err := filepath.Glob(filepath.Join(dir, pattern))
		if err == nil && len(matches) > 0 {
			return matches[0]
		}
	}
	return ""
}

func burnSubtitlesToVideos(ctx context.Context, dir string, videoFiles []string, onLine func(string)) ([]string, string, error) {
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

		subFile := findMatchingSubtitle(dir, vf)
		if subFile == "" {
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
			"-pix_fmt", "yuv420p",
			"-preset", "veryfast",
			"-crf", "20",
			"-c:a", "aac",
			"-b:a", "192k",
			"-ar", "48000",
			"-ac", "2",
			"-movflags", "+faststart",
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

func adjacentCover(ctx context.Context, file string) string {
	base := strings.TrimSuffix(file, filepath.Ext(file))
	for _, ext := range []string{".jpg", ".jpeg", ".png"} {
		p := base + ext
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}
	// If webp exists, convert to jpg for Bilibili compatibility with context timeout
	convCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	webp := base + ".webp"
	if _, err := os.Stat(webp); err == nil {
		jpg := base + ".cover.jpg"
		if _, err := os.Stat(jpg); err == nil {
			return jpg
		}
		_ = exec.CommandContext(convCtx, "ffmpeg", "-y", "-i", webp, jpg).Run()
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
		_ = exec.CommandContext(convCtx, "ffmpeg", "-y", "-i", webpFile, jpgFile).Run()
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

func getClientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		parts := strings.Split(xff, ",")
		if len(parts) > 0 && strings.TrimSpace(parts[0]) != "" {
			return strings.TrimSpace(parts[0])
		}
	}
	if xri := r.Header.Get("X-Real-IP"); xri != "" {
		return strings.TrimSpace(xri)
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err == nil {
		return host
	}
	return r.RemoteAddr
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

	ip := getClientIP(r)
	now := time.Now()

	a.loginMu.Lock()
	if a.loginAttempts == nil {
		a.loginAttempts = make(map[string]*loginAttempt)
	}
	attempt := a.loginAttempts[ip]
	if attempt != nil && now.Before(attempt.lockedTo) {
		a.loginMu.Unlock()
		remaining := int(attempt.lockedTo.Sub(now).Seconds()) + 1
		jsonResp(w, 429, map[string]any{
			"error": fmt.Sprintf("登录失败次数过多，已被临时锁定，请在 %d 秒后再试", remaining),
		})
		return
	}
	a.loginMu.Unlock()

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
		a.loginMu.Lock()
		if a.loginAttempts[ip] == nil {
			a.loginAttempts[ip] = &loginAttempt{}
		}
		att := a.loginAttempts[ip]
		att.count++
		if att.count >= 5 {
			att.lockedTo = now.Add(5 * time.Minute)
			att.count = 0
		}
		a.loginMu.Unlock()
		jsonResp(w, 401, map[string]string{"error": "账号或密码错误"})
		return
	}

	a.loginMu.Lock()
	delete(a.loginAttempts, ip)
	a.loginMu.Unlock()

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
			j, err := a.retryJobManual(id)
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
	if removed := a.compactDuplicateJobs(); removed > 0 {
		fmt.Printf("compacted %d duplicate terminal jobs\n", removed)
	}
	a.recoverTransientJobs()
	a.recoverInterruptedJobs()
	go a.retryWatchdog(context.Background())
	go a.diskRecoveryWatchdog(context.Background())
	if freed := a.cleanupCompletedJobMedia(); freed > 0 {
		fmt.Printf("cleaned %s from completed pipeline jobs\n", formatBytes(freed))
	}
	if freed := a.cleanupOrphanedMedia(); freed > 0 {
		fmt.Printf("cleaned %s from orphaned media directories\n", formatBytes(freed))
	}
	a.loadChannels()
	a.loadStats()
	a.startChannelWatcher(context.Background())
	a.startNetworkSampler(context.Background())
	a.startReviewWatcher(context.Background())

	s := &http.Server{
		Addr:              a.cfg.Addr,
		Handler:           http.HandlerFunc(a.handler),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       60 * time.Second,
		WriteTimeout:      0, // Disabled so streaming large video files via /files/ is not cut off
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
