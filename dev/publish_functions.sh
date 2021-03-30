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

set_version_and_install() {
  version=$(get_version)
  ./build/mvn versions:set -DnewVersion="$version"
  ./build/mvn -DskipTests "${PALANTIR_FLAGS[@]}" install
}

publish_artifacts() {
  tmp_settings="tmp-settings.xml"
  echo "<settings><servers><server>" > $tmp_settings
  echo "<id>sonatype-palantir-release</id><username>$SONATYPE_USERNAME</username>" >> $tmp_settings
  echo "<password>$SONATYPE_PASSWORD</password>" >> $tmp_settings
  echo "</server></servers></settings>" >> $tmp_settings

  ./build/mvn --settings $tmp_settings -DskipTests "${PALANTIR_FLAGS[@]}" -Dgpg.passphrase=$GPG_SIGNING_KEY_PASSWORD deploy
}

make_dist() {
  version=$(get_version)
  hadoop_name="hadoop-palantir"
  artifact_name="spark-dist_2.12-${hadoop_name}"
  file_name="spark-dist-${version}-${hadoop_name}.tgz"
  ./dev/make-distribution.sh --name "hadoop-palantir" --tgz "$@" "${PALANTIR_FLAGS[@]}"
}

make_dist_and_deploy() {
  make_dist
  curl -u $SONATYPE_USERNAME:$SONATYPE_PASSWORD -T "$file_name" "https://oss.sonatype.org/content/palantir/releases/spark/${version}/org/apache/spark/${artifact_name}/${version}/${artifact_name}-${version}.tgz"
  curl -u $SONATYPE_USERNAME:$SONATYPE_PASSWORD -X POST "https://oss.sonatype.org/content/palantir/releases/spark/${version}/publish"
}
