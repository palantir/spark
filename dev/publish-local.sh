#!/usr/bin/env bash

set -euo pipefail
version=$(git describe --tags)

PALANTIR_FLAGS=(-Phadoop-cloud -Phadoop-palantir -Pkinesis-asl -Pkubernetes -Phive -Pyarn -Psparkr)

MVN_LOCAL="~/.m2/repository"

publish_artifacts() {
  ./build/mvn versions:set -DnewVersion=$version
  ./build/mvn -DskipTests "${PALANTIR_FLAGS[@]}" install
}

make_dist() {
  dist_name="$1"
  build_flags="$2"
  shift 2
  dist_version="${version}-${dist_name}"
  file_name="spark-dist-${dist_version}.tgz"
  ./dev/make-distribution.sh --name $dist_name --tgz "$@" $build_flags
  mkdir $MVN_LOCAL/org/apache/spark/spark-dist/${dist_version} && \
  cp -r $file_name $MVN_LOCAL/org/apache/spark/spark-dist/${dist_version}/${file_name}
}

publish_artifacts
make_dist hadoop-2.8.0-palantir3 "${PALANTIR_FLAGS[*]}"
