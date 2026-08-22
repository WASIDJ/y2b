<div align="center">

# y2b

### YouTube / Magnet → Bilibili 的低内存 Go 自动化流水线

<p>
  <img src="https://cdn.simpleicons.org/go/00ADD8" width="42" height="42" alt="Go" title="Go" />
  <img src="https://cdn.simpleicons.org/youtube/FF0000" width="42" height="42" alt="YouTube" title="YouTube" />
  <img src="https://cdn.simpleicons.org/ffmpeg/007808" width="42" height="42" alt="FFmpeg" title="FFmpeg" />
  <img src="https://cdn.simpleicons.org/bilibili/00A1D6" width="42" height="42" alt="Bilibili" title="Bilibili" />
  <img src="https://cdn.simpleicons.org/linux/FCC624" width="42" height="42" alt="Linux" title="Linux" />
  <img src="https://cdn.simpleicons.org/systemd/1DA1F2" width="42" height="42" alt="systemd" title="systemd" />
</p>

![Go](https://img.shields.io/badge/Go-1.22-00ADD8?logo=go&logoColor=white)
![License](https://img.shields.io/badge/license-private%20deployment-64748b)
![Memory](https://img.shields.io/badge/memory--first-低内存-16a34a)
![Self healing](https://img.shields.io/badge/systemd-self--healing-7c3aed)

</div>

## 简介

y2b 是一个面向低内存 VPS 的 Go 媒体编排服务，将 YouTube 或 Magnet 内容下载、章节拆分、字幕保留、Biliup 投稿和频道监控整合到一个中文 Web 控制台中。

设计目标：媒体内容始终由外部工具流式落盘，Go 只管理任务元数据和受限日志；默认不压制硬字幕，直接复用 YouTube 字幕，避免不必要的长时间转码。

## 技术栈

| 模块 | 技术 |
| --- | --- |
| 服务端 | Go · `net/http` · goroutine · context |
| 下载 | `yt-dlp` · `aria2c` |
| 媒体处理 | `ffmpeg` · VTT/SRT/BCC |
| B站投稿 | `biliup` · 多 P · 多提交端点 |
| 前端 | 原生 HTML/CSS/JavaScript · 内嵌单页控制台 |
| 守护 | systemd · 自动重启 · 资源限制 |

## 功能

- YouTube 视频、播放列表和 Magnet 异步下载
- YouTube 字幕、封面、简介保存；默认直接复用字幕投稿
- 章节自动拆分多 P
- Biliup 多 P 投稿、封面、标签、转载来源和 AI 元数据
- 频道监控与自动同步
- 队列状态、取消、重试、删除和日志查看
- 媒体库、磁盘/RAM/CPU/网络和累计流量统计
- 中文 Web 控制台与登录保护
- 原子队列持久化、`.bak` 回退、进程自动恢复

## 字幕策略

默认流程为：

```text
YouTube 字幕 → 保存 / 转换 SRT、BCC → 视频保留字幕 → Biliup open_subtitle
```

默认不会调用 ffmpeg 重新编码视频。只有在请求中显式设置 `burn_subs:true`，或在控制台手动勾选硬字幕压制时，才会执行压制。

## Biliup 自修复策略

投稿失败会按状态分类处理，并且所有路径都有有限边界：

- 标题、简介、标签、分区、封面错误：自动清洗或回退后重试
- `21138`：切换备用投稿接口
- 网络错误和上传限速：使用 Biliup 退避；`601` 不再快速轮询多个端点
- 登录失效、重复稿、每日频控：保护性停止，保留本地文件
- 未知状态码：有限尝试后失败，不会无限循环
- Biliup 外部进程默认 4 小时超时，可用 `Y2B_UPLOAD_TIMEOUT` 调整

BT 防卡队列策略：

- aria2 BT 任务默认总时限 30 分钟，可用 `Y2B_MAGNET_TIMEOUT` 调整
- 下载/投稿等待槽位默认最多 2 小时，可用 `Y2B_QUEUE_WAIT_TIMEOUT` 调整
- BT 无 Peer、无 Seed 或持续 0 速度的失败会标记为 `dead_seed`，不会无限重试
- Magnet URI 必须包含 `xt` 信息哈希；失败重试前应等待种子冷却，避免重复占用队列

BT 单任务提速参数：

- 下载并发仍固定为 1，避免多个 aria2/ffmpeg 进程同时造成内存和磁盘 IO 峰值
- 单任务使用 16M 磁盘缓存、最多 60 个 Peer、每个服务器最多 4 条连接
- 默认监听 BT 端口 `51413`，可用 `Y2B_BT_LISTEN_PORT` 修改
- 请在 VPS 安全组和本机防火墙同时放行该端口的 TCP/UDP；端口不可达时速度可能明显下降
- 下载启用断点续传，死种或网络中断后可复用已经完成的分片

## 快速运行

依赖：`yt-dlp`、`ffmpeg`、`aria2c` 和 `biliup`。

```sh
Y2B_COOKIES=/srv/y2b/youtube_cookies.txt \
Y2B_BILI_COOKIES=/srv/y2b/cookies.json \
Y2B_DATA=/srv/y2b/data \
./y2b-go
```

DeepSeek 元数据增强：

```sh
DEEPSEEK_API_KEY=your_key
DEEPSEEK_MODEL=deepseek-chat
```

建议将 API key 放入 `/etc/y2b.env`，不要写入仓库。

## systemd 部署

```sh
sudo install -m 0644 y2b-go.service /etc/systemd/system/y2b-go.service
sudo systemctl daemon-reload
sudo systemctl enable --now y2b-go
```

默认策略：

- `Restart=always`
- Go 内存上限 64 MiB runtime limit / systemd 350 MiB
- 下载和投稿各自单槽位，避免 VPS OOM
- 任务状态原子写入并保留备份
- 收到 SIGTERM/SIGINT 时优雅退出

## API

控制台：`http://127.0.0.1:8765/`（生产环境建议通过反向代理提供 HTTPS）。

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| `POST` | `/api/youtube/download` | YouTube 下载、字幕、封面和简介 |
| `POST` | `/api/magnet/download` | Magnet 下载 |
| `POST` | `/api/biliup/upload` | 单视频或多 P 投稿 |
| `POST` | `/api/pipeline` | 下载 → 处理 → B站投稿 |
| `GET` | `/api/jobs` | 查询任务队列 |
| `GET` | `/api/jobs/{id}` | 查询单个任务 |
| `POST` | `/api/jobs/{id}/retry` | 重试失败任务 |
| `POST` | `/api/jobs/{id}/cancel` | 取消任务 |
| `GET` | `/health` | 健康检查 |
| `GET` | `/api/system` | 系统与流量诊断 |

示例：

```json
{
  "url": "https://youtu.be/...",
  "sub_langs": "zh-Hans,zh,en",
  "split_chapters": true,
  "burn_subs": false,
  "translate": true
}
```

所有任务异步执行，视频不进入 Go 堆内存，文件落盘到 `Y2B_DATA`。

## 安全与本地文件

以下文件只保留在部署机，不提交到 Git：

- `cookies.json`：B站登录 cookie
- `youtube_cookies.txt`：YouTube cookie
- `auth.cookie`、`test_cookie.txt`
- `data/`、日志、频道配置和构建二进制

`.gitignore` 已默认排除上述内容。请不要将 cookie 或 API key 粘贴到 issue、commit 或公开仓库。

## 开发与验证

```sh
go test ./...
go vet ./...
go build -trimpath -ldflags='-s -w' -o y2b-go main.go
```

仓库内测试包含 Biliup 状态码解析、自修复分类、标题修复、接口回退和 `601` 限速防止端点风暴等 mock 场景。
