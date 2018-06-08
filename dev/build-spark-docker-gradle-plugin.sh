#!/usr/bin/env bash
set -euo pipefail
ROOT=$(git rev-parse --show-toplevel)
cd "$ROOT/resource-managers/kubernetes/docker"
./gradlew --info build
cd -
