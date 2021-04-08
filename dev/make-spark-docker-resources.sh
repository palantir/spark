#!/usr/bin/env bash

set -euo pipefail

# This script zips resources needed to have spark modules run in kubernetes.
docker_resources_temporary_folder="docker_resources_temporary_folder"
mkdir -p ./$docker_resources_temporary_folder/kubernetes/dockerfiles/spark
cp ./resource-managers/kubernetes/docker/src/main/dockerfiles/spark/Dockerfile ./$docker_resources_temporary_folder/kubernetes/dockerfiles/spark/Dockerfile.original
cp ./resource-managers/kubernetes/docker/src/main/dockerfiles/spark/entrypoint.sh ./$docker_resources_temporary_folder/kubernetes/dockerfiles/spark
cp -a bin $docker_resources_temporary_folder/bin
cp -a sbin $docker_resources_temporary_folder/sbin
# We have to get into the folder or the folder itself will be part of the zip
(cd $docker_resources_temporary_folder && zip -r docker-resources.zip bin sbin kubernetes)
mv $docker_resources_temporary_folder/docker-resources.zip docker-resources.zip
