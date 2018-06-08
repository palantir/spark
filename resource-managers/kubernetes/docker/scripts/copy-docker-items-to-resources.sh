#!/bin/bash
set -eux
SPARK_REPO_ROOT=$(git rev-parse --show-toplevel)
SCRIPTS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
MODULE_ROOT_DIR="$(cd "$SCRIPTS_DIR/.." && pwd )"
BUILD_OUTPUT_DIR="$MODULE_ROOT_DIR/target"
DOCKER_BUNDLE_DIR="$BUILD_OUTPUT_DIR/docker-resources"
DOCKER_BUNDLE_ARCHIVE_DIR="$MODULE_ROOT_DIR/src/main/resources"
DOCKER_BUNDLE_ARCHIVE="$DOCKER_BUNDLE_ARCHIVE_DIR/docker-resources.tgz"

rm -rf $DOCKER_BUNDLE_DIR
mkdir -p $DOCKER_BUNDLE_DIR

mkdir -p $DOCKER_BUNDLE_DIR/dockerfile
cp $MODULE_ROOT_DIR/src/main/dockerfiles/spark/Dockerfile $DOCKER_BUNDLE_DIR/dockerfile/.

mkdir -p $DOCKER_BUNDLE_DIR/entrypoint
cp $MODULE_ROOT_DIR/src/main/dockerfiles/spark/entrypoint.sh $DOCKER_BUNDLE_DIR/entrypoint/.

mkdir -p $DOCKER_BUNDLE_DIR/bin
cp -R $SPARK_REPO_ROOT/bin/* $DOCKER_BUNDLE_DIR/bin/.

mkdir -p $DOCKER_BUNDLE_DIR/sbin
cp -R $SPARK_REPO_ROOT/sbin/* $DOCKER_BUNDLE_DIR/sbin/.

if [ -f $DOCKER_BUNDLE_ARCHIVE ];
then
rm $DOCKER_BUNDLE_ARCHIVE
fi

cd $BUILD_OUTPUT_DIR
tar -czvf $DOCKER_BUNDLE_ARCHIVE docker-resources

