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

func TestBiliRepairActionMatrix(t *testing.T) {
	cases := map[int]biliRepairAction{
		0: biliRepairSuccess, -101: biliRepairStop,
		21016: biliRepairStop, 21017: biliRepairStop, 21018: biliRepairStop,
		21020: biliRepairTitle, 21021: biliRepairTitle, 21022: biliRepairTitle,
		21023: biliRepairDesc, 21024: biliRepairDesc, 21025: biliRepairDesc,
		21030: biliRepairTags, 21031: biliRepairTags, 21033: biliRepairTags,
		21040: biliRepairTID, 21041: biliRepairTID, 21042: biliRepairTID,
		21050: biliRepairCover, 21051: biliRepairCover, 21052: biliRepairCover,
		21070: biliRepairStop, 21071: biliRepairStop, 21564: biliRepairStop,
		21138: biliRepairSwitch,
		601:   biliRepairRateLimit, 99999: biliRepairUnknown,
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
