#!/bin/bash

source ./config.sh

docker run \
   -e WM_MODE="prod" \
   -e WM_DATA_PIPELINE_ADDR="http://10.65.18.57:4200" \
   --name wm-request-queue \
   --rm \
   -p 4040:4040 \
   $DOCKER_IMAGE:$DOCKER_IMAGE_VERSION
