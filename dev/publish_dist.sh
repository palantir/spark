#!/usr/bin/env bash

set -euo pipefail

FWDIR="$(cd "`dirname "${BASH_SOURCE[0]}"`"; pwd)"

source "$FWDIR/publish_functions.sh"

DONT_BUILD=true make_dist_and_deploy | tee -a "/tmp/make-dist.log"
