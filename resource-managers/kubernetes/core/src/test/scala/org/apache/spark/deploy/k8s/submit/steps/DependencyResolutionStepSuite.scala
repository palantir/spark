/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.deploy.k8s.submit.steps

import io.fabric8.kubernetes.api.model.{ContainerBuilder, HasMetadata, PodBuilder}

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.submit.KubernetesDriverSpec

class DependencyResolutionStepSuite extends SparkFunSuite {

  private val SPARK_FILES = Seq(
    "apps/files/file1.txt",
    "local:///var/apps/files/file2.txt")

  private val CONFIGURED_FILES_DOWNLOAD_PATH = "/var/data/spark-submitted-files"

  test("Added dependencies should be resolved in Spark configuration and environment") {
    val dependencyResolutionStep = new DependencyResolutionStep(
      SPARK_FILES)
    val driverPod = new PodBuilder().build()
    val baseDriverSpec = KubernetesDriverSpec(
      driverPod = driverPod,
      driverContainer = new ContainerBuilder().build(),
      driverSparkConf = new SparkConf(false)
        .set(FILES_DOWNLOAD_LOCATION, CONFIGURED_FILES_DOWNLOAD_PATH),
      otherKubernetesResources = Seq.empty[HasMetadata])
    val preparedDriverSpec = dependencyResolutionStep.configureDriver(baseDriverSpec)
    assert(preparedDriverSpec.driverPod === driverPod)
    assert(preparedDriverSpec.otherKubernetesResources.isEmpty)
    val resolvedSparkFiles = preparedDriverSpec.driverSparkConf.get("spark.files").split(",").toSet
    val expectedResolvedSparkFiles = Set(
      s"$CONFIGURED_FILES_DOWNLOAD_PATH/file1.txt",
      "/var/apps/files/file2.txt")
    assert(resolvedSparkFiles === expectedResolvedSparkFiles)
  }
}
