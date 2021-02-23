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
package org.apache.spark.deploy.k8s.features

import java.io.File

import scala.collection.JavaConverters._

import com.google.common.base.Charsets
import com.google.common.io.{BaseEncoding, Files}
import io.fabric8.kubernetes.api.model.Secret
import org.scalatest.BeforeAndAfter

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.k8s.{KubernetesDriverConf, SparkPod}
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.submit.JavaMainAppResource
import org.apache.spark.util.Utils

class MountLocalFilesFeatureStepSuite extends SparkFunSuite with BeforeAndAfter {

  private var kubernetesConf: KubernetesDriverConf = _
  private var sparkFiles: Seq[String] = _
  private var localFiles: Seq[File] = _
  private var stepUnderTest: MountLocalFilesFeatureStep = _

  before {
    val tempDir = Utils.createTempDir()
    val firstLocalFile = new File(tempDir, "file1.txt")
    Files.write("a", firstLocalFile, Charsets.UTF_8)
    val secondLocalFile = new File(tempDir, "file2.txt")
    Files.write("b", secondLocalFile, Charsets.UTF_8)
    sparkFiles = Seq(
      firstLocalFile.getAbsolutePath,
      s"file://${secondLocalFile.getAbsolutePath}",
      s"local://file3.txt",
      "https://localhost:9000/file4.txt")
    localFiles = Seq(firstLocalFile, secondLocalFile)
    val sparkConf = new SparkConf(false)
      .set("spark.files", sparkFiles.mkString(","))
      .set(KUBERNETES_SECRET_FILE_MOUNT_ENABLED, true)
      .set(KUBERNETES_SECRET_FILE_MOUNT_PATH, "/var/data/spark-submitted-files")
    kubernetesConf = new KubernetesDriverConf(
      sparkConf,
      "test-app",
      JavaMainAppResource(None),
      "main",
      Array.empty[String])
    stepUnderTest = new MountLocalDriverFilesFeatureStep(kubernetesConf)
  }

  test("Attaches a secret volume and secret name") {
    val configuredPod = stepUnderTest.configurePod(SparkPod.initialPod())
    assert(configuredPod.pod.getSpec.getVolumes.size === 1)
    val volume = configuredPod.pod.getSpec.getVolumes.get(0)
    assert(volume.getName === "submitted-files")
    assert(volume.getSecret.getSecretName === s"${kubernetesConf.resourceNamePrefix}-mounted-files")
    assert(configuredPod.container.getVolumeMounts.size === 1)
    val volumeMount = configuredPod.container.getVolumeMounts.get(0)
    assert(volumeMount.getName === "submitted-files")
    assert(volumeMount.getMountPath === "/var/data/spark-submitted-files")
    assert(configuredPod.container.getEnv.size === 1)
    val addedEnv = configuredPod.container.getEnv.get(0)
    assert(addedEnv.getName === ENV_MOUNTED_FILES_FROM_SECRET_DIR)
    assert(addedEnv.getValue === "/var/data/spark-submitted-files")
  }

  test("Maps submitted files in the system properties") {
    val resolvedSystemProperties = stepUnderTest.getAdditionalPodSystemProperties()
    val expectedSystemProperties = Map(
      "spark.files" ->
        Seq(
          "/var/data/spark-submitted-files/file1.txt",
          "/var/data/spark-submitted-files/file2.txt",
          "local://file3.txt",
          "https://localhost:9000/file4.txt"
    ).mkString(","))
    assert(resolvedSystemProperties === expectedSystemProperties)
  }

  test("Additional Kubernetes resources includes the mounted files secret") {
    val secrets = stepUnderTest.getAdditionalKubernetesResources()
    assert(secrets.size === 1)
    assert(secrets.head.isInstanceOf[Secret])
    val secret = secrets.head.asInstanceOf[Secret]
    assert(secret.getMetadata.getName === s"${kubernetesConf.resourceNamePrefix}-mounted-files")
    val secretData = secret.getData.asScala
    assert(secretData.size === 2)
    assert(decodeToUtf8(secretData("file1.txt")) === "a")
    assert(decodeToUtf8(secretData("file2.txt")) === "b")
  }

  private def decodeToUtf8(str: String): String = {
    new String(BaseEncoding.base64().decode(str), Charsets.UTF_8)
  }
}
