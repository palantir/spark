#!/usr/bin/env bash

set -euo pipefail

PALANTIR_FLAGS=(-Psparkr -Phadoop-palantir)

get_version() {
  git describe --tags --first-parent
}

get_release_type() {
  [ -n "${CIRCLE_TAG-}" ] && echo "release" || echo "snapshot"
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
  # Set maven credentials
  tmp_settings="tmp-settings.xml"
  echo "<settings><servers><server>" > $tmp_settings
  echo "<id>internal-palantir-repository</id><username>$ARTIFACTORY_USERNAME</username>" >> $tmp_settings
  echo "<password>$ARTIFACTORY_PASSWORD</password>" >> $tmp_settings
  echo "</server></servers></settings>" >> $tmp_settings

  # We need to point at a different push repo based on the fact that it's a release or a snapshot
  ./build/mvn -DaltDeploymentRepository=internal-palantir-repository::https://publish.artifactory.palantir.build/artifactory/internal-jar-fork-$(get_release_type) --settings $tmp_settings -DskipTests "${PALANTIR_FLAGS[@]}" deploy
}

make_dist() {
  version=$(get_version)
  hadoop_name="hadoop-palantir"
  artifact_name="spark-dist_2.12-${hadoop_name}"
  file_name="spark-dist-${version}-${hadoop_name}.tgz"
  ./dev/make-distribution.sh --name "hadoop-palantir" --tgz "$@" "${PALANTIR_FLAGS[@]}"
}

make_dist_and_deploy() {
  # The dist is a tar containing all needed to run spark. Used by pyspark-conda to extract python sources.
  make_dist
  curl -u $ARTIFACTORY_USERNAME:$ARTIFACTORY_PASSWORD -T "$file_name" "https://publish.artifactory.palantir.build/artifactory/internal-dist-fork-$(get_release_type)/org/apache/spark/${artifact_name}/${version}/${artifact_name}-${version}.tgz"
}
