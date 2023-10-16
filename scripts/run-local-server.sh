#!/bin/bash
LATEST_IMAGE=$(docker images | grep osml-tile-server | awk 'NR==1{print $1":"$2}')

docker run \
  -p 80:80 \
  -v `pwd`/local_viewpoint_cache:/tmp/viewpoint:rw \
  --env AWS_ACCESS_KEY_ID --env AWS_SECRET_ACCESS_KEY --env AWS_SESSION_TOKEN \
  ${LATEST_IMAGE}
