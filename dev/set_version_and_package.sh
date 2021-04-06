#!/usr/bin/env bash

set -euo pipefail

get_version() {
  git describe --tags --first-parent
}

set_version_and_package() {
  version=$(get_version)
  ./build/mvn versions:set -DnewVersion="$version"
  ./build/mvn -DskipTests "${PALANTIR_FLAGS[@]}" package
}

FWDIR="$(cd "`dirname "${BASH_SOURCE[0]}"`"; pwd)"

set_version_and_package
