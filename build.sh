#!/bin/bash
set -e

REGISTRY="${REGISTRY:-}"
TAG="${TAG:-latest}"

SERVER_IMAGE="file-sync-server"
CLIENT_IMAGE="file-sync-client"

if [ -n "$REGISTRY" ]; then
  SERVER_IMAGE="${REGISTRY}/${SERVER_IMAGE}"
  CLIENT_IMAGE="${REGISTRY}/${CLIENT_IMAGE}"
fi

cd "$(dirname "$0")"

echo "==> 构建服务端镜像: ${SERVER_IMAGE}:${TAG}"
docker build -t "${SERVER_IMAGE}:${TAG}" -f Dockerfile.server .

echo "==> 构建客户端镜像: ${CLIENT_IMAGE}:${TAG}"
docker build -t "${CLIENT_IMAGE}:${TAG}" -f Dockerfile.client .

echo ""
echo "构建完成!"
echo "  服务端: ${SERVER_IMAGE}:${TAG}"
echo "  客户端: ${CLIENT_IMAGE}:${TAG}"
echo ""
echo "运行示例:"
echo "  docker run -d --name file-sync-server -p 8080:8080 -v /data/sync:/data ${SERVER_IMAGE}:${TAG}"
echo "  docker run -d --name file-sync-client -p 8081:8081 -v /data/local:/data ${CLIENT_IMAGE}:${TAG}"
