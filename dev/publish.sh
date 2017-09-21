#!/usr/bin/env bash

set -euo pipefail
version=$(git describe --tags)

PALANTIR_FLAGS=(-Phadoop-cloud -Phadoop-palantir -Pkinesis-asl -Pkubernetes -Phive -Pyarn -Psparkr)

publish_artifacts() {
  tmp_settings="tmp-settings.xml"
  echo "<settings><servers><server>" > $tmp_settings
  echo "<id>bintray-palantir-release</id><username>$BINTRAY_USERNAME</username>" >> $tmp_settings
  echo "<password>$BINTRAY_PASSWORD</password>" >> $tmp_settings
  echo "</server></servers></settings>" >> $tmp_settings

  ./build/mvn versions:set -DnewVersion=$version
  ./build/mvn --settings $tmp_settings -DskipTests "${PALANTIR_FLAGS[@]}" deploy
}

make_dist() {
  build_flags="$1"
  shift 1
  artifact_name="spark-dist_2.11-hadoop-palantir"
  file_name="${artifact_name}-${version}.tgz"
  ./dev/make-distribution.sh --name "hadoop-palantir" --tgz "$@" $build_flags
  curl -u $BINTRAY_USERNAME:$BINTRAY_PASSWORD -T $file_name "https://api.bintray.com/content/palantir/releases/spark/${version}/org/apache/spark/${artifact_name}/${version}/${file_name}"
}

publish_artifacts
make_dist "${PALANTIR_FLAGS[*]}" --clean
