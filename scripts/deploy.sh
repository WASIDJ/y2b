#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

echo "=== [1/4] Running test suite ==="
go test -v ./...

echo "=== [2/4] Compiling y2b-go ==="
COMMIT=$(git rev-parse --short HEAD 2>/dev/null || echo "unknown")
BUILD_TIME=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
VERSION="1.2.0"

go build -ldflags "-s -w -X main.Version=${VERSION} -X main.BuildCommit=${COMMIT} -X main.BuildTime=${BUILD_TIME}" -o y2b-go main.go
echo "Built y2b-go (${VERSION}@${COMMIT} at ${BUILD_TIME})"

echo "=== [3/4] Checking active processes before reload ==="
if pgrep -f biliup >/dev/null 2>&1; then
    echo "WARNING: biliup process is actively running in background!"
fi

echo "=== [4/4] Restarting y2b-go.service ==="
sudo systemctl restart y2b-go.service
sleep 1

if sudo systemctl is-active --quiet y2b-go.service; then
    echo "Service y2b-go is running successfully."
    curl -s http://127.0.0.1:8765/health | grep -o '"version":"[^"]*"' || true
else
    echo "ERROR: Service failed to start! Checking journalctl logs..."
    journalctl -u y2b-go -n 20 --no-pager
    exit 1
fi

echo "=== Deployment Completed Successfully! ==="
