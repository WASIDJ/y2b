package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestAcquireSlotTimesOut(t *testing.T) {
	slot := make(chan struct{}, 1)
	slot <- struct{}{}
	a := &App{cfg: Config{QueueWaitTimeout: 10 * time.Millisecond}}
	err := a.acquireSlot(context.Background(), slot)
	if err == nil || !strings.Contains(err.Error(), "队列等待超时") {
		t.Fatalf("expected queue timeout, got %v", err)
	}
}

func TestProgressParsing(t *testing.T) {
	if got := parseSpeedBytes("1.5MiB/s"); got != int64(1.5*1024*1024) {
		t.Fatalf("unexpected speed: %d", got)
	}
	if got := parseETASeconds("01:02"); got != 62 {
		t.Fatalf("unexpected eta: %d", got)
	}
	a := &App{}
	j := &Job{ID: "progress-test", Status: "running"}
	a.progressLine(j, "YouTube 下载", "download: 42.5%|4250|10000|2MiB/s|12|demo")
	if j.Progress == nil || j.Progress.Percent != 42.5 || j.Progress.Downloaded != 4250 || j.Progress.Total != 10000 || j.Progress.ETASeconds != 12 {
		t.Fatalf("yt-dlp progress was not parsed: %+v", j.Progress)
	}
	a.progressLine(j, "BT 下载", "[#abc 42% 4MiB/10MiB(40%) CN:2 DL:2MiB ETA:12s]")
	if j.Progress == nil || j.Progress.Percent != 42 {
		t.Fatalf("aria2 progress was not parsed: %+v", j.Progress)
	}
}

func TestRunCmdProgressStreamsLines(t *testing.T) {
	var lines []string
	logs, err := runCmdProgress(context.Background(), "/bin/sh", []string{"-c", "printf 'download: 12%%|12|100|1MiB/s|8|demo\\n'"}, func(line string) {
		lines = append(lines, line)
	})
	if err != nil || len(lines) != 1 || !strings.Contains(logs, "download: 12%") {
		t.Fatalf("streaming command output failed: lines=%v logs=%q err=%v", lines, logs, err)
	}
}

func TestRetryReplacesTerminalRecord(t *testing.T) {
	a := &App{
		cfg:  Config{DataDir: filepath.Join(t.TempDir(), "not-created")},
		jobs: map[string]*Job{},
	}
	old := &Job{ID: "old-job", Kind: "unknown", Status: "failed", Input: map[string]any{"url": "test"}}
	a.jobs[old.ID] = old
	a.order = []string{old.ID}

	retried, err := a.retryJob(old.ID)
	if err != nil {
		t.Fatalf("retry failed: %v", err)
	}
	if retried.ID == old.ID || retried.Status != "queued" {
		t.Fatalf("unexpected retried job: %+v", retried)
	}
	if len(a.jobs) != 1 || len(a.order) != 1 || a.jobs[old.ID] != nil || a.order[0] != retried.ID {
		t.Fatalf("retry left a duplicate record: jobs=%d order=%v", len(a.jobs), a.order)
	}
}

func TestRetryCollapsesDuplicateTerminalRecords(t *testing.T) {
	a := &App{cfg: Config{DataDir: filepath.Join(t.TempDir(), "not-created")}, jobs: map[string]*Job{}}
	input := map[string]any{"url": "same"}
	first := &Job{ID: "first", Kind: "unknown", Status: "failed", Input: input}
	second := &Job{ID: "second", Kind: "unknown", Status: "canceled", Input: input}
	a.jobs[first.ID] = first
	a.jobs[second.ID] = second
	a.order = []string{first.ID, second.ID}

	retried, err := a.retryJob(first.ID)
	if err != nil {
		t.Fatalf("retry failed: %v", err)
	}
	if len(a.jobs) != 1 || len(a.order) != 1 || a.jobs[retried.ID] == nil || retried.Status != "queued" {
		t.Fatalf("duplicate terminal records were not collapsed: jobs=%d order=%v", len(a.jobs), a.order)
	}
}

func TestShortVideoDisablesChapterSplitting(t *testing.T) {
	bin, _ := writeMockBiliup(t, `echo '{"duration": 1799}'`)
	a := &App{cfg: Config{YTDLP: bin}}
	split, log := a.chapterSplitDecision(context.Background(), "https://www.youtube.com/watch?v=test", "", true)
	if split || !strings.Contains(log, "小于 30 分钟") {
		t.Fatalf("short video should not split chapters: split=%v log=%q", split, log)
	}
}

func TestCompactDuplicateJobsKeepsOneVideoRecord(t *testing.T) {
	a := &App{cfg: Config{DataDir: filepath.Join(t.TempDir(), "not-created")}, jobs: map[string]*Job{}}
	input := map[string]any{"url": "https://www.youtube.com/watch?v=video-1", "tags": "one"}
	a.jobs["failed"] = &Job{ID: "failed", Kind: "pipeline", Status: "failed", Created: time.Unix(1, 0), Input: input}
	a.jobs["done"] = &Job{ID: "done", Kind: "pipeline", Status: "done", Created: time.Unix(2, 0), Input: map[string]any{"url": input["url"], "tags": "two"}}
	a.order = []string{"failed", "done"}
	if removed := a.compactDuplicateJobs(); removed != 1 {
		t.Fatalf("expected one duplicate removed, got %d", removed)
	}
	if len(a.jobs) != 1 || a.jobs["done"] == nil {
		t.Fatalf("completed record was not preserved: %+v", a.jobs)
	}
}

func TestPurgeVideoFilesInDirRemovesAllVideoVariants(t *testing.T) {
	dir := t.TempDir()
	video := filepath.Join(dir, "merged.mp4")
	chapter := filepath.Join(dir, "P01.mp4")
	note := filepath.Join(dir, "description.txt")
	for _, path := range []string{video, chapter, note} {
		if err := os.WriteFile(path, []byte("data"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if freed := purgeVideoFilesInDir(dir); freed != 8 {
		t.Fatalf("unexpected freed bytes: %d", freed)
	}
	if _, err := os.Stat(note); err != nil {
		t.Fatalf("non-video sidecar was removed: %v", err)
	}
	for _, path := range []string{video, chapter} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("video file remains: %s", path)
		}
	}
}

func TestCleanupWaitsForReviewApproval(t *testing.T) {
	dir := t.TempDir()
	video := filepath.Join(dir, "video.mp4")
	if err := os.WriteFile(video, []byte("video"), 0600); err != nil {
		t.Fatal(err)
	}
	a := &App{jobs: map[string]*Job{"pending": {
		ID: "pending", Kind: "pipeline", Status: "done", ReviewState: "pending",
		Output: map[string]any{"dir": dir},
	}}, order: []string{"pending"}}
	if freed := a.cleanupCompletedJobMedia(); freed != 0 {
		t.Fatalf("pending review must not clean media: freed=%d", freed)
	}
	if _, err := os.Stat(video); err != nil {
		t.Fatalf("pending media was removed: %v", err)
	}
	a.jobs["pending"].ReviewState = "passed"
	if freed := a.cleanupCompletedJobMedia(); freed != int64(len("video")) {
		t.Fatalf("approved review should clean media: freed=%d", freed)
	}
}

func TestMagnetValidationAndDeadSeedClassification(t *testing.T) {
	if !validTorrentOrMagnet("magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567") {
		t.Fatal("expected valid magnet URI")
	}
	if validTorrentOrMagnet("magnet:?dn=missing-info-hash") {
		t.Fatal("magnet without xt should be rejected")
	}
	if !isDeadSeedOutput("[ERROR] number of seeders: 0") {
		t.Fatal("expected dead seed output to be classified")
	}
}

func TestMediaRecognitionSupportsCommonContainersAndAudio(t *testing.T) {
	for _, name := range []string{"movie.mp4", "movie.MKV", "movie.ts", "movie.m2ts", "movie.mxf", "movie.rmvb", "audio.m4a", "audio.flac"} {
		if !isVideoFilePath(name) {
			t.Errorf("expected media format to be recognized: %s", name)
		}
	}
	for _, name := range []string{"cover.jpg", "captions.vtt", "video.mp4.part", "notes.txt"} {
		if isVideoFilePath(name) {
			t.Errorf("unexpected non-media file recognition: %s", name)
		}
	}
}

func TestMediaFilesAndSizesAreDeduplicated(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "video.mp4")
	if err := os.WriteFile(file, []byte("123456"), 0600); err != nil {
		t.Fatal(err)
	}
	files := uniqueMediaFiles([]string{file, file, filepath.Join(dir, ".", "video.mp4"), filepath.Join(dir, "notes.txt")})
	if len(files) != 1 || calcFilesSize(append(files, file)) != 6 {
		t.Fatalf("media paths were not deduplicated: files=%v size=%d", files, calcFilesSize(append(files, file)))
	}
}

func TestAriaSelectFileValidation(t *testing.T) {
	for _, value := range []string{"", "3", "3,7-9", "1-4,8"} {
		if !validAriaSelectFile(value) {
			t.Errorf("valid aria2 file selection rejected: %q", value)
		}
	}
	for _, value := range []string{"all", "3;7", "../3", "3,,7"} {
		if validAriaSelectFile(value) {
			t.Errorf("invalid aria2 file selection accepted: %q", value)
		}
	}
}

func TestSingleAriaSelectFileDetection(t *testing.T) {
	if !isSingleAriaSelectFile("3") || isSingleAriaSelectFile("3,7-9") || isSingleAriaSelectFile("3-4") {
		t.Fatal("single aria2 file selection detection is incorrect")
	}
}

func TestMagnetStreamingUploadProcessesCompletedFiles(t *testing.T) {
	dir := t.TempDir()
	aria := filepath.Join(dir, "aria2-mock.sh")
	if err := os.WriteFile(aria, []byte("#!/bin/sh\nprintf video > \"$1/part.mp4\"\necho done\n"), 0700); err != nil {
		t.Fatal(err)
	}
	a := &App{cfg: Config{Aria2: aria}}
	var uploaded []string
	logs, files, err := a.runMagnetStreamingUpload(context.Background(), []string{dir}, dir, nil, func(file string) error {
		uploaded = append(uploaded, file)
		return nil
	})
	if err != nil || !strings.Contains(logs, "done") || len(files) != 1 || len(uploaded) != 1 {
		t.Fatalf("streaming upload failed: logs=%q files=%v uploaded=%v err=%v", logs, files, uploaded, err)
	}
}

func TestBiliRepairActionMatrix(t *testing.T) {
	cases := map[int]biliRepairAction{
		0: biliRepairSuccess, -101: biliRepairStop,
		21016: biliRepairStop, 21017: biliRepairStop, 21018: biliRepairStop,
		21020: biliRepairTitle, 21021: biliRepairTitle, 21022: biliRepairTitle,
		21023: biliRepairDesc, 21024: biliRepairDesc, 21025: biliRepairDesc,
		21030: biliRepairTags, 21031: biliRepairTags, 21033: biliRepairTags,
		21040: biliRepairTID, 21041: biliRepairTID, 21042: biliRepairTID,
		21050: biliRepairCover, 21051: biliRepairCover, 21052: biliRepairCover,
		21070: biliRepairStop, 21071: biliRepairStop,
		21138: biliRepairSwitch,
		406:   biliRepairRateLimit, 601: biliRepairRateLimit, 21564: biliRepairRateLimit, 21085: biliRepairRateLimit,
		99999: biliRepairUnknown,
	}
	for code, want := range cases {
		if got := biliRepairActionFor(code); got != want {
			t.Errorf("code %d: got %q, want %q", code, got, want)
		}
	}
}

func TestParseBiliupOutput(t *testing.T) {
	got := parseBiliupOutput(`message: "upload rate limit" (code: 601)`)
	if got.Code != 601 || got.Message != "upload rate limit" {
		t.Fatalf("parsed unexpected result: %+v", got)
	}
	got = parseBiliupOutput(`code: 0\nBV1AbCDeFgH1 投稿成功`)
	if got.Code != 0 || got.BVID != "BV1AbCDeFgH1" {
		t.Fatalf("success parse failed: %+v", got)
	}
}

func TestParseBiliReviewAndViolations(t *testing.T) {
	result, err := parseBiliReviewOutput("INFO tracing\n{\"archive\":{\"state\":-2,\"state_desc\":\"已退回\",\"reject_reason\":\"您的视频【P7(00:07:37-00:08:23)】【P11内容全程】存在问题\"}}")
	if err != nil || biliReviewState(result) != "rejected" {
		t.Fatalf("review result was not parsed: result=%+v err=%v", result, err)
	}
	violations := parseReviewViolations(result.Archive.RejectReason)
	if len(violations) != 2 || violations[0].Part != 7 || violations[0].Start != 457 || violations[0].End != 503 || !violations[1].Whole || violations[1].Part != 11 {
		t.Fatalf("unexpected violations: %+v", violations)
	}
	passed, err := parseBiliReviewOutput("{\"archive\":{\"state_desc\":\"已通过\"}}")
	if err != nil || biliReviewState(passed) != "passed" {
		t.Fatalf("passed review result was not recognized: %+v err=%v", passed, err)
	}
}

func writeMockBiliup(t *testing.T, body string) (string, string) {
	t.Helper()
	dir := t.TempDir()
	bin := filepath.Join(dir, "biliup-mock.sh")
	state := filepath.Join(dir, "calls")
	if err := os.WriteFile(bin, []byte("#!/bin/sh\n"+body), 0700); err != nil {
		t.Fatal(err)
	}
	return bin, state
}

func TestBiliupAutoRepairMock(t *testing.T) {
	bin, state := writeMockBiliup(t, `
n=0
[ -f "$MOCK_STATE" ] && n=$(cat "$MOCK_STATE")
n=$((n+1)); echo "$n" > "$MOCK_STATE"
if [ "$n" -eq 1 ]; then
  echo 'message: "invalid title" code: 21020'
else
  echo 'code: 0 BV1AbCDeFgH1 投稿成功'
fi
`)
	video := filepath.Join(t.TempDir(), "video.mp4")
	if err := os.WriteFile(video, []byte("test"), 0600); err != nil {
		t.Fatal(err)
	}
	a := &App{cfg: Config{Biliup: bin, BiliCookies: "unused"}}
	oldState := os.Getenv("MOCK_STATE")
	if err := os.Setenv("MOCK_STATE", state); err != nil {
		t.Fatal(err)
	}
	defer os.Setenv("MOCK_STATE", oldState)
	out, logs, err := a.executeBiliupUpload(context.Background(), uploadReq{File: video, Title: strings.Repeat("标题", 50)})
	if err != nil {
		t.Fatalf("expected repaired mock upload to succeed: %v; logs=%s", err, logs)
	}
	if out["bvid"] != "BV1AbCDeFgH1" || !strings.Contains(logs, "捕获标题问题") {
		t.Fatalf("repair result missing: out=%v logs=%s", out, logs)
	}
	b, _ := os.ReadFile(state)
	if strings.TrimSpace(string(b)) != "2" {
		t.Fatalf("expected one repair retry, calls=%q", b)
	}
}

func TestBiliupRateLimitStopsEndpointStorm(t *testing.T) {
	bin, state := writeMockBiliup(t, `
n=0
[ -f "$MOCK_STATE" ] && n=$(cat "$MOCK_STATE")
n=$((n+1)); echo "$n" > "$MOCK_STATE"
echo 'message: "upload rate limit" (code: 601)'
exit 1
`)
	video := filepath.Join(t.TempDir(), "video.mp4")
	if err := os.WriteFile(video, []byte("test"), 0600); err != nil {
		t.Fatal(err)
	}
	a := &App{cfg: Config{Biliup: bin, BiliCookies: "unused"}}
	oldState := os.Getenv("MOCK_STATE")
	if err := os.Setenv("MOCK_STATE", state); err != nil {
		t.Fatal(err)
	}
	defer os.Setenv("MOCK_STATE", oldState)
	_, logs, err := a.executeBiliupUpload(context.Background(), uploadReq{File: video})
	if err == nil || !strings.Contains(err.Error(), "601") || !strings.Contains(logs, "停止快速切换线路") {
		t.Fatalf("rate-limit protection missing: err=%v logs=%s", err, logs)
	}
	b, _ := os.ReadFile(state)
	if strings.TrimSpace(string(b)) != "1" {
		t.Fatalf("rate limit should stop after one endpoint, calls=%q", b)
	}
}

func TestBiliupJSONRateLimitIsClassified(t *testing.T) {
	got := parseBiliupOutput(`RuntimeError: {"OK":0,"info":"您上传视频过快，请您稍作休息后再继续","code":406,"message":"您上传视频过快，请您稍作休息后再继续"}`)
	if got.Code != 406 || got.Message == "" {
		t.Fatalf("JSON biliup error was not parsed: %+v", got)
	}
	if biliRepairActionFor(got.Code) != biliRepairRateLimit {
		t.Fatalf("code 406 was not classified as rate limited")
	}
	if got := classifyFailure("biliup exit status 1", got.RawLogs); got != "upload_rate_limit" {
		t.Fatalf("failure category = %q, want upload_rate_limit", got)
	}
}

func TestFailureCategoriesOnlyRetryTransientErrors(t *testing.T) {
	if got := classifyFailure("dead_seed: no seed", ""); got != "dead_seed" {
		t.Fatalf("dead seed category = %q", got)
	}
	if isAutoRetryableCategory("dead_seed") {
		t.Fatal("dead seed must not be auto-retried")
	}
	if !isAutoRetryableCategory("queue_timeout") || !isAutoRetryableCategory("upload_rate_limit") {
		t.Fatal("transient categories should be auto-retryable")
	}
	// Rate limit retries must not be capped
	if !autoRetryAllowed(5, 10, "upload_rate_limit") {
		t.Fatal("upload_rate_limit must remain auto-retryable indefinitely")
	}
	// YouTube bot challenge is retryable
	if got := classifyFailure("Sign in to confirm you're not a bot. Use --cookies-from-browser", ""); got != "youtube_bot_challenge" {
		t.Fatalf("YouTube bot challenge category = %q, want youtube_bot_challenge", got)
	}
	if !isAutoRetryableCategory("youtube_bot_challenge") {
		t.Fatal("youtube_bot_challenge must be auto-retryable")
	}
	// Missing media (biliup os error 2) is retryable to allow re-download fallback
	if got := classifyFailure("RuntimeError: No such file or directory (os error 2)", ""); got != "missing_media" {
		t.Fatalf("missing media category = %q, want missing_media", got)
	}
	if !isAutoRetryableCategory("missing_media") {
		t.Fatal("missing_media must be auto-retryable to enable re-download fallback")
	}
}


func TestBiliupEndpointFallbackMock(t *testing.T) {
	bin, state := writeMockBiliup(t, `
n=0
[ -f "$MOCK_STATE" ] && n=$(cat "$MOCK_STATE")
n=$((n+1)); echo "$n" > "$MOCK_STATE"
if [ "$n" -eq 1 ]; then
  echo 'message: "web endpoint unavailable" code: 21138'
else
  echo 'code: 0 BV1AbCDeFgH1 投稿成功'
fi
`)
	video := filepath.Join(t.TempDir(), "video.mp4")
	if err := os.WriteFile(video, []byte("test"), 0600); err != nil {
		t.Fatal(err)
	}
	a := &App{cfg: Config{Biliup: bin, BiliCookies: "unused"}}
	oldState := os.Getenv("MOCK_STATE")
	if err := os.Setenv("MOCK_STATE", state); err != nil {
		t.Fatal(err)
	}
	defer os.Setenv("MOCK_STATE", oldState)
	_, logs, err := a.executeBiliupUpload(context.Background(), uploadReq{File: video})
	if err != nil || !strings.Contains(logs, "切换备用 Biliup 提交通道") {
		t.Fatalf("endpoint fallback failed: err=%v logs=%s", err, logs)
	}
	b, _ := os.ReadFile(state)
	if strings.TrimSpace(string(b)) != "2" {
		t.Fatalf("expected fallback attempt, calls=%q", b)
	}
}

func TestBiliupRepairExhaustionRemainsFailed(t *testing.T) {
	bin, _ := writeMockBiliup(t, `echo 'message: "invalid title" code: 21020'`)
	video := filepath.Join(t.TempDir(), "video.mp4")
	if err := os.WriteFile(video, []byte("test"), 0600); err != nil {
		t.Fatal(err)
	}
	a := &App{cfg: Config{Biliup: bin, BiliCookies: "unused"}}
	out, logs, err := a.executeBiliupUpload(context.Background(), uploadReq{
		File:  video,
		Title: strings.Repeat("标题", 50),
	})
	if err == nil {
		t.Fatalf("exhausted repair attempts must fail: out=%v logs=%s", out, logs)
	}
	if !strings.Contains(err.Error(), "已尝试 3 个提交通道") {
		t.Fatalf("unexpected exhaustion error: %v", err)
	}
}

func TestMultiPartUploadTranslatesPartTitles(t *testing.T) {
	bin, argsFile := writeMockBiliup(t, `
printf '%s\n' "$@" > "$MOCK_STATE"
echo 'code: 0 BV1AbCDeFgH1 投稿成功'
`)
	partDir := t.TempDir()
	partOne := filepath.Join(partDir, "Episode 01 - The Beginning.mp4")
	partTwo := filepath.Join(partDir, "Episode 02 - The Return.mp4")
	for _, file := range []string{partOne, partTwo} {
		if err := os.WriteFile(file, []byte("video"), 0600); err != nil {
			t.Fatal(err)
		}
	}

	llm := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"{\"title\":\"中文分P标题\",\"summary\":\"\",\"tags\":[],\"tid\":\"188\"}"}}]}`))
	}))
	defer llm.Close()

	a := &App{cfg: Config{Biliup: bin, BiliCookies: "unused", DeepSeekKey: "test", DeepSeekURL: llm.URL}}
	oldState := os.Getenv("MOCK_STATE")
	if err := os.Setenv("MOCK_STATE", argsFile); err != nil {
		t.Fatal(err)
	}
	defer os.Setenv("MOCK_STATE", oldState)

	out, logs, err := a.executeBiliupUpload(context.Background(), uploadReq{
		Files:     []string{partOne, partTwo},
		Translate: true,
		Parts:     true,
	})
	if err != nil {
		t.Fatalf("expected multi-part upload to succeed: %v; logs=%s", err, logs)
	}
	if out["translated_parts"] != 2 || !strings.Contains(logs, "[分P标题翻译] P1") || !strings.Contains(logs, "[分P标题翻译] P2") {
		t.Fatalf("part translation result missing: out=%v logs=%s", out, logs)
	}
	args, err := os.ReadFile(argsFile)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(args), "P01 - 中文分P标题.mp4") || !strings.Contains(string(args), "P02 - 中文分P标题.mp4") {
		t.Fatalf("translated part filenames were not passed to biliup: %s", args)
	}
	if _, err := os.Stat(partOne); err != nil {
		t.Fatalf("original part was unexpectedly removed: %v", err)
	}
}

func TestDeleteMediaHandlerPathTraversal(t *testing.T) {
	dataDir := t.TempDir()
	a := &App{cfg: Config{DataDir: dataDir}}

	cases := []struct {
		folder string
		want   int
	}{
		{folder: dataDir + "/youtube/abc", want: 200},
		{folder: dataDir, want: 403},
		{folder: dataDir + "-evil", want: 403},
		{folder: "/etc/passwd", want: 403},
		{folder: dataDir + "/../etc", want: 403},
	}

	_ = os.MkdirAll(dataDir+"/youtube/abc", 0750)

	for _, tc := range cases {
		body := strings.NewReader(`{"folder":"` + tc.folder + `"}`)
		req := httptest.NewRequest("DELETE", "/api/media", body)
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		a.deleteMediaHandler(w, req)
		if w.Code != tc.want {
			t.Errorf("folder %q: got HTTP %d, want %d", tc.folder, w.Code, tc.want)
		}
	}
}

func TestLoadOrCreateSecretKey(t *testing.T) {
	dir := t.TempDir()
	key1 := loadOrCreateSecretKey(dir)
	if len(key1) < 32 {
		t.Fatalf("generated key too short: %q", key1)
	}
	key2 := loadOrCreateSecretKey(dir)
	if key1 != key2 {
		t.Fatalf("key changed between calls: %q vs %q", key1, key2)
	}
}

func TestChannelsPersistenceBakRecovery(t *testing.T) {
	dataDir := t.TempDir()
	a := &App{
		cfg:          Config{DataDir: dataDir},
		channels:     map[string]*MonitoredChannel{},
		channelOrder: nil,
	}
	ch := &MonitoredChannel{ID: "ch1", URL: "https://www.youtube.com/@test", Title: "Test Channel"}
	a.channels[ch.ID] = ch
	a.channelOrder = []string{ch.ID}
	a.saveChannels()

	// Trigger second save with valid content to create .bak containing the valid previous version
	ch.Title = "Updated Title"
	a.saveChannels()

	// Corrupt channels.json
	channelsFile := a.channelsFilePath()
	if err := os.WriteFile(channelsFile, []byte("{corrupted json"), 0640); err != nil {
		t.Fatal(err)
	}

	// Load should recover from .bak
	a2 := &App{cfg: Config{DataDir: dataDir}}
	a2.loadChannels()
	if len(a2.channels) != 1 || a2.channels["ch1"] == nil {
		t.Fatalf("channels not recovered from .bak: %+v", a2.channels)
	}
}

func TestStatsPersistenceBakRecovery(t *testing.T) {
	dataDir := t.TempDir()
	a := &App{cfg: Config{DataDir: dataDir}}
	a.recordDownload(1024)
	a.recordUpload(2048)

	// Save once more to create .bak
	a.recordDownload(512)

	// Corrupt main stats.json
	if err := os.WriteFile(a.statsFile(), []byte("not valid json"), 0640); err != nil {
		t.Fatal(err)
	}

	a2 := &App{cfg: Config{DataDir: dataDir}}
	a2.loadStats()
	if a2.stats.TotalDownloadedBytes == 0 && a2.stats.TotalUploadedBytes == 0 {
		t.Fatalf("stats not recovered from .bak: %+v", a2.stats)
	}
}

func TestClearFinishedJobsProtectsPendingReview(t *testing.T) {
	a := &App{
		cfg:  Config{DataDir: t.TempDir()},
		jobs: map[string]*Job{},
	}
	jNormal := &Job{ID: "j1", Status: "done", ReviewState: ""}
	jPassed := &Job{ID: "j2", Status: "done", ReviewState: "passed"}
	jPending := &Job{ID: "j3", Status: "done", ReviewState: "pending"}
	jFailed := &Job{ID: "j4", Status: "failed"}

	a.jobs[jNormal.ID] = jNormal
	a.jobs[jPassed.ID] = jPassed
	a.jobs[jPending.ID] = jPending
	a.jobs[jFailed.ID] = jFailed
	a.order = []string{jNormal.ID, jPassed.ID, jPending.ID, jFailed.ID}

	cleared := a.clearFinishedJobs()
	if cleared != 3 {
		t.Fatalf("expected 3 jobs cleared, got %d", cleared)
	}
	if len(a.jobs) != 1 || a.jobs["j3"] == nil {
		t.Fatalf("pending review job was not preserved: %+v", a.jobs)
	}

	// Also verify deleteJob prevents deleting pending review job
	if err := a.deleteJob("j3"); err == nil {
		t.Fatal("expected error deleting job awaiting review approval")
	}
}

func TestMagnetCleanupOnReviewApproval(t *testing.T) {
	dir := t.TempDir()
	video := filepath.Join(dir, "torrent_video.mkv")
	if err := os.WriteFile(video, []byte("torrent-content"), 0600); err != nil {
		t.Fatal(err)
	}
	a := &App{jobs: map[string]*Job{"mag1": {
		ID: "mag1", Kind: "magnet", Status: "done", ReviewState: "pending",
		Output: map[string]any{"dir": dir, "stream_upload": true},
	}}, order: []string{"mag1"}}

	if freed := a.cleanupCompletedJobMedia(); freed != 0 {
		t.Fatalf("pending magnet review must not clean media: freed=%d", freed)
	}
	if _, err := os.Stat(video); err != nil {
		t.Fatalf("pending magnet media was removed prematurely: %v", err)
	}

	a.jobs["mag1"].ReviewState = "passed"
	if freed := a.cleanupCompletedJobMedia(); freed != int64(len("torrent-content")) {
		t.Fatalf("approved magnet review should clean media: freed=%d", freed)
	}
	if _, err := os.Stat(video); !os.IsNotExist(err) {
		t.Fatalf("magnet video file should have been deleted: %v", err)
	}
}

func TestAdjacentCoverContext(t *testing.T) {
	dir := t.TempDir()
	videoFile := filepath.Join(dir, "sample.mp4")
	_ = os.WriteFile(videoFile, []byte("video"), 0600)

	// Case 1: JPG cover exists
	jpgCover := filepath.Join(dir, "sample.jpg")
	_ = os.WriteFile(jpgCover, []byte("jpg-data"), 0600)
	if got := adjacentCover(context.Background(), videoFile); got != jpgCover {
		t.Errorf("expected jpg cover %s, got %s", jpgCover, got)
	}

	// Case 2: Canceled context does not hang
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_ = adjacentCover(ctx, filepath.Join(dir, "nonexistent.mp4"))
}

func TestConvertVttToSrtAndBccRobustness(t *testing.T) {
	dir := t.TempDir()
	vttContent := `WEBVTT

1
00:00:01.000 --> 00:00:04.000
First subtitle line <c.colorCCCCCC>with tag</c>
and second line

2
00:00:05.500 --> 00:00:08.000
Second subtitle without gap
00:00:08.000 --> 00:00:10.000
Third subtitle immediate next
`
	vttFile := filepath.Join(dir, "video.zh-Hans.vtt")
	if err := os.WriteFile(vttFile, []byte(vttContent), 0600); err != nil {
		t.Fatal(err)
	}

	convertVttToSrtAndBcc(dir)

	srtFile := filepath.Join(dir, "video.zh-Hans.srt")
	srtBytes, err := os.ReadFile(srtFile)
	if err != nil {
		t.Fatalf("expected srt file to be generated: %v", err)
	}
	srtContent := string(srtBytes)
	if !strings.Contains(srtContent, "First subtitle line with tag") || !strings.Contains(srtContent, "Second subtitle without gap") || !strings.Contains(srtContent, "Third subtitle immediate next") {
		t.Fatalf("srt missing expected cues: %s", srtContent)
	}

	bccFile := filepath.Join(dir, "video.zh-Hans.bcc")
	bccBytes, err := os.ReadFile(bccFile)
	if err != nil {
		t.Fatalf("expected bcc file to be generated: %v", err)
	}
	if !strings.Contains(string(bccBytes), "Third subtitle immediate next") {
		t.Fatalf("bcc missing cues: %s", string(bccBytes))
	}
}

func TestBuildYTDLPAndAria2ArgsHelpers(t *testing.T) {
	ytArgs := buildYTDLPArgs("https://youtu.be/test", "1080p", "zh", "/path/to/cookies.txt", false, false, "/tmp/dir")
	if len(ytArgs) == 0 {
		t.Fatal("empty yt-dlp args")
	}
	joined := strings.Join(ytArgs, " ")
	if !strings.Contains(joined, "--no-playlist") || !strings.Contains(joined, "--cookies /path/to/cookies.txt") {
		t.Fatalf("unexpected yt args: %s", joined)
	}

	ariaArgs := buildAria2Args("magnet:?xt=urn:btih:123", "/tmp/dir", "1", "6881")
	joinedAria := strings.Join(ariaArgs, " ")
	if !strings.Contains(joinedAria, "--select-file=1") || !strings.Contains(joinedAria, "--listen-port=6881") || !strings.Contains(joinedAria, "--enable-dht=true") || !strings.Contains(joinedAria, "--dht-entry-point=") {
		t.Fatalf("unexpected aria args: %s", joinedAria)
	}
}

func TestLoginRateLimiting(t *testing.T) {
	a := &App{
		cfg: Config{
			AdminUser: "admin",
			AdminPass: "correct-password",
			SecretKey: "secret-key-123456789012345678901234",
		},
	}

	badBody := `{"username":"admin","password":"wrong-password"}`
	for i := 0; i < 5; i++ {
		req := httptest.NewRequest("POST", "/api/login", strings.NewReader(badBody))
		w := httptest.NewRecorder()
		a.loginHandler(w, req)
		if w.Code != 401 {
			t.Fatalf("attempt %d: expected 401, got %d", i+1, w.Code)
		}
	}

	// 6th attempt should be locked out with 429 Too Many Requests
	req := httptest.NewRequest("POST", "/api/login", strings.NewReader(badBody))
	w := httptest.NewRecorder()
	a.loginHandler(w, req)
	if w.Code != 429 {
		t.Fatalf("expected 429 Too Many Requests after 5 failed attempts, got %d", w.Code)
	}

	// Even correct password is locked out
	goodBody := `{"username":"admin","password":"correct-password"}`
	req2 := httptest.NewRequest("POST", "/api/login", strings.NewReader(goodBody))
	w2 := httptest.NewRecorder()
	a.loginHandler(w2, req2)
	if w2.Code != 429 {
		t.Fatalf("expected locked IP to be rejected with 429 even with correct password, got %d", w2.Code)
	}
}

func TestScanMediaPackagesCaching(t *testing.T) {
	dir := t.TempDir()
	a := &App{cfg: Config{DataDir: dir}}

	// First scan
	pkgs1 := a.scanMediaPackages()
	if len(pkgs1) != 0 {
		t.Fatalf("expected empty packages, got %d", len(pkgs1))
	}

	// Add a new package directory
	_ = os.MkdirAll(filepath.Join(dir, "youtube", "vid1"), 0750)
	_ = os.WriteFile(filepath.Join(dir, "youtube", "vid1", "vid.mp4"), []byte("data"), 0600)

	// Immediate second scan should hit cache
	pkgs2 := a.scanMediaPackages()
	if len(pkgs2) != 0 {
		t.Fatalf("expected cached empty result within 3s, got %d", len(pkgs2))
	}
}

func TestCleanupOrphanedMedia(t *testing.T) {
	dir := t.TempDir()
	a := &App{
		cfg: Config{DataDir: dir},
		jobs: map[string]*Job{
			"active1": {ID: "active1", Status: "running"},
		},
	}

	// 1. Active directory
	activeDir := filepath.Join(dir, "magnet", "active1")
	_ = os.MkdirAll(activeDir, 0750)
	_ = os.WriteFile(filepath.Join(activeDir, "video.mp4"), []byte("active data"), 0600)

	// 2. Orphan directory
	orphanDir := filepath.Join(dir, "magnet", "orphan99")
	_ = os.MkdirAll(orphanDir, 0750)
	_ = os.WriteFile(filepath.Join(orphanDir, "video.mp4"), []byte("orphan data"), 0600)

	freed := a.cleanupOrphanedMedia()
	if freed == 0 {
		t.Fatalf("expected freed bytes > 0, got %d", freed)
	}

	// Verify orphan was deleted
	if _, err := os.Stat(orphanDir); !os.IsNotExist(err) {
		t.Fatalf("expected orphan dir to be deleted, but still exists: %v", err)
	}
	// Verify active dir was preserved
	if _, err := os.Stat(activeDir); os.IsNotExist(err) {
		t.Fatalf("expected active dir to be preserved, but was deleted: %v", err)
	}
}

func TestEnsureSafeDisk(t *testing.T) {
	dir := t.TempDir()
	a := &App{
		cfg: Config{
			DataDir:       dir,
			MinFreeDiskGB: 0.001, // Small threshold that current filesystem satisfies
		},
		jobs: map[string]*Job{},
	}

	if err := a.ensureSafeDisk(context.Background()); err != nil {
		t.Fatalf("expected disk check to pass on normal fs, got error: %v", err)
	}

	// Test with impossibly high min free disk requirement
	a.cfg.MinFreeDiskGB = 999999.0
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if err := a.ensureSafeDisk(ctx); err == nil {
		t.Fatalf("expected error when disk space requirement is impossibly large")
	}
}

func TestResumableJobUploadPreservesFilesAndVID(t *testing.T) {
	dir := t.TempDir()
	v1 := filepath.Join(dir, "P01 - Lecture 1.mp4")
	v2 := filepath.Join(dir, "P02 - Lecture 2.mp4")
	_ = os.WriteFile(v1, []byte("vid1"), 0600)
	_ = os.WriteFile(v2, []byte("vid2"), 0600)

	// Case 1: Failed pipeline with completed media and existing BVID
	failedJob := &Job{
		ID:     "pipe123",
		Kind:   "pipeline",
		Status: "failed",
		Input: pipelineReq{
			URL:       "https://youtu.be/test",
			Translate: true,
			Tid:       "188",
			Tags:      "AI,Tutorial",
		},
		Output: map[string]any{
			"dir":         dir,
			"video_files": []string{v1, v2},
			"upload": map[string]any{
				"bvid":        "BV1xx411c7mD",
				"title":       "My Custom Title",
				"description": "My Custom Desc",
			},
		},
	}

	req, ok := resumableJobUpload(failedJob)
	if !ok {
		t.Fatalf("expected resumableJobUpload to return true")
	}
	if len(req.Files) != 2 || req.VID != "BV1xx411c7mD" || !req.Translate || req.Title != "My Custom Title" {
		t.Fatalf("unexpected uploadReq from resumableJobUpload: %+v", req)
	}

	// Case 2: retryJob converts pipeline into biliup with resume state
	a := &App{
		jobs: map[string]*Job{
			failedJob.ID: failedJob,
		},
		order: []string{failedJob.ID},
	}
	newJob, err := a.retryJob(failedJob.ID)
	if err != nil {
		t.Fatalf("retryJob failed: %v", err)
	}
	if newJob.Kind != "biliup" {
		t.Fatalf("expected retryJob to convert completed media into biliup kind, got %s", newJob.Kind)
	}
	upInput, ok := newJob.Input.(uploadReq)
	if !ok || len(upInput.Files) != 2 || upInput.VID != "BV1xx411c7mD" {
		t.Fatalf("unexpected newJob input: %+v", newJob.Input)
	}

	// Case 3: When local files are deleted, retryJob automatically falls back to pipeline
	_ = os.Remove(v1)
	_ = os.Remove(v2)
	fallbackApp := &App{
		jobs: map[string]*Job{
			failedJob.ID: failedJob,
		},
		order: []string{failedJob.ID},
	}
	fallbackJob, err := fallbackApp.retryJob(failedJob.ID)
	if err != nil {
		t.Fatalf("retryJob fallback failed: %v", err)
	}
	if fallbackJob.Kind != "pipeline" {
		t.Fatalf("expected fallback to pipeline when media files are missing, got %s", fallbackJob.Kind)
	}
	// Case 4: Synthetic recovery job resolves original URL through related job ID
	origPipeline := &Job{
		ID:     "orig8a7c",
		Kind:   "pipeline",
		Status: "failed",
		Input: pipelineReq{
			URL:       "magnet:?xt=urn:btih:8a7cef125593ba70&dn=TestCourse",
			Translate: true,
			Tid:       "188",
			Tags:      "Tutorial",
		},
	}
	syntheticBiliup := &Job{
		ID:     "synth123",
		Kind:   "biliup",
		Status: "failed",
		Input: uploadReq{
			Source: "magnet-recovery-orig8a7c",
			Title:  "Part 1",
			Files:  []string{"/tmp/missing/file.mp4"},
		},
	}
	synthApp := &App{
		jobs: map[string]*Job{
			origPipeline.ID:    origPipeline,
			syntheticBiliup.ID: syntheticBiliup,
		},
		order: []string{origPipeline.ID, syntheticBiliup.ID},
	}
	recoveredJob, err := synthApp.retryJob(syntheticBiliup.ID)
	if err != nil {
		t.Fatalf("synthetic retryJob failed: %v", err)
	}
	if recoveredJob.Kind != "pipeline" {
		t.Fatalf("expected synthetic job to recover as pipeline, got %s", recoveredJob.Kind)
	}
	recInput, ok := recoveredJob.Input.(pipelineReq)
	if !ok || recInput.URL != "magnet:?xt=urn:btih:8a7cef125593ba70&dn=TestCourse" {
		t.Fatalf("unexpected recovered pipeline URL: %+v", recoveredJob.Input)
	}
}









