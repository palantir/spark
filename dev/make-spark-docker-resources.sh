#!/usr/bin/env bash

set -euo pipefail

get_version() {
  git describe --tags --first-parent
}

# This script zips resources needed to have spark modules run in kubernetes.
base_folder="spark-docker-resources-$(get_version)"
mkdir -p ./$base_folder/kubernetes/dockerfiles/spark
cp ./resource-managers/kubernetes/docker/src/main/dockerfiles/spark/Dockerfile ./$base_folder/kubernetes/dockerfiles/spark/Dockerfile.original
cp ./resource-managers/kubernetes/docker/src/main/dockerfiles/spark/entrypoint.sh ./$base_folder/kubernetes/dockerfiles/spark
cp -a bin ./$base_folder
cp -a sbin ./$base_folder
zip -r docker-resources.zip $base_folder
