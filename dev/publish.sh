#!/usr/bin/env bash

set -euo pipefail
version=$(git describe --tags)
docker_repo=hub.docker.com

PALANTIR_FLAGS=(-Phadoop-cloud -Phadoop-palantir -Pkinesis-asl -Pkubernetes -Phive -Pyarn -Psparkr)

publish_artifacts() {
  echo "Publishing artifacts..."
  tmp_settings="tmp-settings.xml"
  echo "<settings><servers><server>" > $tmp_settings
  echo "<id>bintray-palantir-release</id><username>$BINTRAY_USERNAME</username>" >> $tmp_settings
  echo "<password>$BINTRAY_PASSWORD</password>" >> $tmp_settings
  echo "</server></servers></settings>" >> $tmp_settings

  ./build/mvn versions:set -DnewVersion=$version
  ./build/mvn --settings $tmp_settings -DskipTests "${PALANTIR_FLAGS[@]}" deploy
}

make_and_publish_dist() {
  echo "Publishing dist $1 $2 ..."
  dist_name="$1"
  build_flags="$2"
  shift 2
  dist_version="${version}-${dist_name}"
  file_name="spark-dist-${dist_version}.tgz"
  ./dev/make-distribution.sh --name $dist_name --tgz "$@" $build_flags
  #curl -u $BINTRAY_USERNAME:$BINTRAY_PASSWORD -T $file_name "https://api.bintray.com/content/palantir/releases/spark/${version}/org/apache/spark/spark-dist/${dist_version}/${file_name}"
}

make_and_publish_docker_images() {
  echo "Publishing docker images..."
  dist_name="$1"

  dist_version="${version}-${dist_name}"
  file_name="spark-dist-${dist_version}.tgz"
  dist_dir="dist-$dist_version/"
  tar -zxvf "$file_name"
  pushd "$dist_dir"

  docker build -t "$docker_repo/spark-driver:$version" -f dockerfiles/driver/Dockerfile .
  docker build -t "$docker_repo/spark-executor:$version" -f dockerfiles/executor/Dockerfile .

  docker login -u "$DOCKER_HUB_USERNAME" -p "$DOCKER_HUB_PASSWORD" "$docker_repo"
  docker push "$docker_repo/spark-driver:$version"
  docker push "$docker_repo/spark-executor:$version"

  popd
}

publish_artifacts
make_and_publish_dist hadoop-2.8.0-palantir3 "${PALANTIR_FLAGS[*]}" --clean
make_and_publish_dist without-hadoop "-Phadoop-provided -Pkubernetes -Phive -Pyarn -Psparkr" --clean
make_and_publish_docker_images hadoop-2.8.0-palantir3
