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
  # Create a local maven deploy such that publish can grab it and sync it with remote repository afterwards
  mkdir local_deploy
  version=$(get_version)
  ./build/mvn versions:set -DnewVersion="$version"
  ./build/mvn -DaltDeploymentRepository=staging-repository::file://local_deploy -DskipTests "${PALANTIR_FLAGS[@]}" deploy
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

  # Push the local maven deploy created by set_version_and_package task
  # We need to point at a different push repo based on the fact that it's a release or a snapshot
  ./build/mvn org.codehaus.mojo:wagon-maven-plugin:2.0.2:merge-maven-repos \
    -Dwagon.targetId=internal-palantir-repository \
    -Dwagon.source=file://local_deploy \
    -Dwagon.target=https://publish.artifactory.palantir.build/artifactory/internal-jar-fork-$(get_release_type) \
    --settings $tmp_settings
}

publish_spark_docker_resources() {
  ./dev/make-spark-docker-resources.sh
  spark_docker_tmp_settings="spark-docker-settings.xml"
  echo "<settings><servers><server>" > $spark_docker_tmp_settings
  echo "<id>internal-palantir-repository</id><username>$ARTIFACTORY_USERNAME</username>" >> $spark_docker_tmp_settings
  echo "<password>$ARTIFACTORY_PASSWORD</password>" >> $spark_docker_tmp_settings
  echo "</server></servers></settings>" >> $spark_docker_tmp_settings
  ./build/mvn deploy:deploy-file \
    -DgroupId=org.apache.spark \
    -DartifactId=spark-docker-resources \
    -Dversion=$(get_version) \
    -Dpackaging=zip \
    -Dfile=docker-resources.zip \
    -DrepositoryId=internal-palantir-repository \
    -Durl=https://publish.artifactory.palantir.build/artifactory/internal-dist-fork-$(get_release_type) \
    --settings $spark_docker_tmp_settings
}

make_dist() {
  version=$(get_version)
  hadoop_name="hadoop-palantir"
  artifact_name="spark-dist_2.12-${hadoop_name}"
  file_name="spark-dist-${version}-${hadoop_name}.tgz"
  pom_file_name="dists/hadoop-palantir/pom.xml"
  ./dev/make-distribution.sh --name "hadoop-palantir" --tgz "$@" "${PALANTIR_FLAGS[@]}"
}

make_dist_and_deploy() {
  # The dist is a tar containing all needed to run spark. Used by pyspark-conda to extract python sources.
  make_dist
  curl -u $ARTIFACTORY_USERNAME:$ARTIFACTORY_PASSWORD -T "$file_name" "https://publish.artifactory.palantir.build/artifactory/internal-dist-fork-$(get_release_type)/org/apache/spark/${artifact_name}/${version}/${artifact_name}-${version}.tgz"
  curl -u $ARTIFACTORY_USERNAME:$ARTIFACTORY_PASSWORD -T "$pom_file_name" "https://publish.artifactory.palantir.build/artifactory/internal-dist-fork-$(get_release_type)/org/apache/spark/${artifact_name}/${version}/${artifact_name}-${version}.pom"
}
