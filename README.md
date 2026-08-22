# y2b-go

轻量 Go 编排服务。媒体内容始终由外部工具流式落盘，Go 进程只保存队列元数据和受限日志，不把视频读入内存，适合低内存 VPS。

凭据文件保持原样：B站 `cookies.json`、YouTube `youtube_cookies.txt`（或通过环境变量指定的 cookie 文件），以及 API key 不会被程序覆盖。YouTube 浏览器 JSON cookie 只会在单个任务目录生成临时 Netscape 文件，任务结束自动删除。

## 运行

```sh
Y2B_COOKIES=/srv/y2b/youtube_cookies.txt \
Y2B_BILI_COOKIES=/srv/y2b/cookies.json \
Y2B_DATA=/srv/y2b/data ./y2b-go
```

依赖：`yt-dlp`、`ffmpeg`、`aria2c`；上传依赖 `biliup`。先执行 `biliup --user-cookie /srv/y2b/cookies.json login`。YouTube cookie 可用 `Y2B_COOKIES` 指定 Netscape/浏览器 JSON 文件，服务会在任务目录临时转换浏览器 JSON，不修改源文件。DeepSeek 可通过 `DEEPSEEK_API_KEY` 和 `DEEPSEEK_MODEL` 配置，systemd 部署时放进 `/etc/y2b.env`。

systemd 部署：复制 `y2b-go.service` 到 `/etc/systemd/system/`，执行 `systemctl daemon-reload && systemctl enable --now y2b-go`。服务异常退出会自动重启；队列写入采用临时文件和 `.bak` 回退，重启不会丢失已完成任务记录。

Biliup 投稿有有限自修复策略：参数类状态码会清洗参数后重试，`21138` 会切换备用提交通道，网络/上传限速由 Biliup 自身退避；账号失效、重复稿、每日频控会保护性停止，不会无限撞接口。每次投稿有 `Y2B_UPLOAD_TIMEOUT` 超时保护，默认 4 小时。

## API

打开 `https://y2b.jeffkafka.top/` 可使用内置中文控制台；页面会自动轮询任务状态。

- `POST /api/youtube/download`：`{"url":"https://youtu.be/...","sub_langs":"all"}`。保存视频、字幕、封面和简介；默认不压制硬字幕，`burn_subs:true` 才会启用 ffmpeg 压制。
- `POST /api/magnet/download`：`{"magnet":"magnet:?xt=..."}`。
- `POST /api/biliup/upload`：`{"file":"/path/a.mp4","files":["/path/p1.mp4","/path/p2.mp4"],"title":"...","description":"...","translate":true,"parts":true}`。`files` 会作为 Biliup 多 P 投稿，简介按 Unicode 截断到 200 字符；`parts:true` 默认沿用 Biliup 的 3 路并发策略。
- YouTube 自动流水线默认直接上传 YouTube 字幕（视频内嵌字幕 + Biliup `open_subtitle`），不重新编码视频；只有请求显式设置 `burn_subs:true` 才会压制硬字幕。
- `GET /api/jobs/{id}`：查询异步任务。
- `GET /api/jobs`：按创建时间倒序查看队列，可用 `?status=queued|running|done|failed|canceled` 筛选。
- `POST /api/jobs/{id}/cancel`：取消排队或运行中的任务，并终止对应外部进程。
- `POST /api/jobs/{id}/retry`：重试失败或已取消任务。
- `DELETE /api/jobs/{id}`：删除已完成、失败或取消的任务记录。

所有任务为异步执行，响应 HTTP 202；任务文件落盘到 `Y2B_DATA`，不在 Go 堆中缓存媒体。
