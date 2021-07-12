#!/bin/bash

source ./config.sh

# builds distil docker image
docker build -t $DOCKER_IMAGE:$DOCKER_IMAGE_VERSION ..
