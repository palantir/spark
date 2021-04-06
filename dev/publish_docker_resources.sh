#!/usr/bin/env bash

set -euo pipefail

FWDIR="$(cd "`dirname "${BASH_SOURCE[0]}"`"; pwd)"

source "$FWDIR/publish_functions.sh"

publish_spark_docker_resources | tee -a "/tmp/publish_spark_docker_resources.log"
