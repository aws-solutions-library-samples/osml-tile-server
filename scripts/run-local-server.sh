#!/bin/bash
set -e

IMAGE_NAME="osml-tile-server"
IMAGE_TAG=$(docker images --format "{{.Tag}}" --filter=reference="$IMAGE_NAME" | head -n 1)

if [[ -z $IMAGE_TAG ]]; then
  echo "Error: No such image: ${IMAGE_NAME}"
  exit 1
fi

CONTAINER_NAME="osml-tile-server-container"

# Check if a container with the same name is already running
# shellcheck disable=SC2046
if [ $(docker ps --filter=name=${CONTAINER_NAME} | wc -l) -gt 1 ]; then
  echo "Error: Container with the name '${CONTAINER_NAME}' is already running."
  exit 1
fi

DOCKER_OPTS=(
  --name "$CONTAINER_NAME"
  -p 80:80
  -v "/tmp/local_viewpoint_cache:/tmp/viewpoint:rw"
  --env AWS_ACCESS_KEY_ID
  --env AWS_SECRET_ACCESS_KEY
  --env AWS_SESSION_TOKEN
  --restart "unless-stopped"
  --log-opt max-size=10m --log-opt max-file=3
)

docker run "${DOCKER_OPTS[@]}" "${IMAGE_NAME}:${IMAGE_TAG}"
