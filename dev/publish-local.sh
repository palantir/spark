#!/usr/bin/env bash

set -euo pipefail
VERSION=$(git describe --tags)
# get the hadoop version from the hadoop-palantir profile from pom.xml, and trim whitespaces
HADOOP_VERSION=$(sed -ne '/<id>hadoop-palantir<\/id>/,/<\/profile>/s/<hadoop.version>\(.*\)<\/hadoop.version>/\1/p' ./pom.xml | tr -d '[:space:]')

PALANTIR_FLAGS=(-Phadoop-cloud -Phadoop-palantir -Pkinesis-asl -Pkubernetes -Phive -Pyarn -Psparkr)

MVN_LOCAL="~/.m2/repository"

publish_artifacts() {
  ./build/mvn versions:set -DnewVersion=$VERSION
  ./build/mvn -DskipTests "${PALANTIR_FLAGS[@]}" install clean
}

make_dist() {
  dist_name="$1"
  build_flags="$2"
  shift 2
  dist_version="${VERSION}-${dist_name}"
  file_name="spark-dist-${dist_version}.tgz"
  ./dev/make-distribution.sh --name $dist_name --tgz "$@" $build_flags
  mkdir -p $MVN_LOCAL/org/apache/spark/spark-dist/${dist_version} && \
  cp $file_name $MVN_LOCAL/org/apache/spark/spark-dist/${dist_version}/
}

publish_artifacts
make_dist hadoop-$HADOOP_VERSION "${PALANTIR_FLAGS[*]}"
