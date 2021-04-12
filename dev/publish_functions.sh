#!/usr/bin/env bash

set -euo pipefail

PALANTIR_FLAGS=(-Psparkr -Phadoop-palantir)

get_version() {
  git describe --tags --first-parent
}

set_version_and_package() {
  version=$(get_version)
  ./build/mvn versions:set -DnewVersion="$version"
  ./build/mvn -DskipTests "${PALANTIR_FLAGS[@]}" package
}
