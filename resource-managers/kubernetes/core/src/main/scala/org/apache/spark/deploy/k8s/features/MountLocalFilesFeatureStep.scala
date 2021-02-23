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
import java.net.URI
import java.nio.file.Paths

import scala.collection.JavaConverters._

import com.google.common.io.{BaseEncoding, Files}
import io.fabric8.kubernetes.api.model.{ContainerBuilder, HasMetadata, PodBuilder, SecretBuilder}

import org.apache.spark.deploy.k8s.{KubernetesConf, KubernetesDriverConf, SparkPod}
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.submit.{JavaMainAppResource, PythonMainAppResource, RMainAppResource}
import org.apache.spark.util.Utils

private[spark] class MountLocalDriverFilesFeatureStep(conf: KubernetesDriverConf)
  extends MountLocalFilesFeatureStep(conf) {

  val allFiles: Seq[String] = {
    Utils.stringToSeq(conf.sparkConf.get("spark.files", "")) ++
      (conf.mainAppResource match {
        case JavaMainAppResource(_) => Nil
        case PythonMainAppResource(res) => Seq(res)
        case RMainAppResource(res) => Seq(res)
      })
  }
}

private[spark] class MountLocalExecutorFilesFeatureStep(conf: KubernetesConf)
  extends MountLocalFilesFeatureStep(conf) {

  val allFiles: Seq[String] = Nil
}

private[spark] abstract class MountLocalFilesFeatureStep(conf: KubernetesConf)
  extends KubernetesFeatureConfigStep {

  private val enabled = conf.get(KUBERNETES_SECRET_FILE_MOUNT_ENABLED)

  private val secretName = s"${conf.resourceNamePrefix}-mounted-files"

  private val mountPath = conf.get(KUBERNETES_SECRET_FILE_MOUNT_PATH)

  override def configurePod(pod: SparkPod): SparkPod = {
    if (!enabled) return pod

    val resolvedPod = new PodBuilder(pod.pod)
      .editOrNewSpec()
        .addNewVolume()
          .withName("submitted-files")
          .withNewSecret()
            .withSecretName(secretName)
            .endSecret()
          .endVolume()
        .endSpec()
      .build()
    val resolvedContainer = new ContainerBuilder(pod.container)
      .addNewEnv()
        .withName(ENV_MOUNTED_FILES_FROM_SECRET_DIR)
        .withValue(mountPath)
        .endEnv()
      .addNewVolumeMount()
        .withName("submitted-files")
        .withMountPath(mountPath)
        .endVolumeMount()
      .build()
    SparkPod(resolvedPod, resolvedContainer)
  }

  override def getAdditionalPodSystemProperties(): Map[String, String] = {
    if (!enabled) return Map.empty

    val resolvedFiles = allFiles()
      .map(file => {
        val uri = Utils.resolveURI(file)
        if (shouldMountFile(uri)) {
          val fileName = Paths.get(uri.getPath).getFileName.toString
          s"$mountPath/$fileName"
        } else {
          file
        }
      })
    Map("spark.files" -> resolvedFiles.mkString(","))
  }

  override def getAdditionalKubernetesResources(): Seq[HasMetadata] = {
    if (!enabled) return Nil

    val localFiles = allFiles()
      .map(Utils.resolveURI)
      .filter(shouldMountFile)
      .map(_.getPath)
      .map(new File(_))
    val localFileBase64Contents = localFiles.map { file =>
      val fileBase64 = BaseEncoding.base64().encode(Files.toByteArray(file))
      (file.getName, fileBase64)
    }.toMap
    val localFilesSecret = new SecretBuilder()
      .withNewMetadata()
        .withName(secretName)
        .endMetadata()
      .withData(localFileBase64Contents.asJava)
      .build()
    Seq(localFilesSecret)
  }

  def allFiles(): Seq[String]

  private def shouldMountFile(file: URI): Boolean = {
    Option(file.getScheme) match {
      case Some("file") => true
      case None => true
      case _ => false
    }
  }
}
