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
package org.apache.spark.deploy.kubernetes.docker.gradle;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.zip.GZIPInputStream;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.gradle.api.DefaultTask;
import org.gradle.api.tasks.TaskAction;

public class DeployScriptsTask extends DefaultTask {

  private File dockerBuildDir;

  public void setDockerBuildDir(File dockerBuildDir) {
    this.dockerBuildDir = dockerBuildDir;
  }

  public File getDockerBuildDir() {
    return dockerBuildDir;
  }

  @TaskAction
  public void deployScripts() {
    File currentDockerBuildDir = getDockerBuildDir();
    File binDir = new File(currentDockerBuildDir, "bin");
    File sbinDir = new File(currentDockerBuildDir, "sbin");
    File entrypointDir = Paths.get(
        currentDockerBuildDir.getAbsolutePath(),
        "kubernetes",
        "dockerfiles",
        "spark").toFile();
    deleteAndMkDirs(binDir);
    deleteAndMkDirs(sbinDir);
    deleteAndMkDirs(entrypointDir);
    try (InputStream dockerResourcesInputStream =
             getClass().getResourceAsStream("/docker-resources.tgz");
         GZIPInputStream dockerResourcesGZipped = new GZIPInputStream(dockerResourcesInputStream);
         TarArchiveInputStream dockerResourcesTar =
             new TarArchiveInputStream(dockerResourcesGZipped)) {
      TarArchiveEntry nextEntry = dockerResourcesTar.getNextTarEntry();
      while (nextEntry != null) {
        boolean extracted = maybeCopyTarEntry(dockerResourcesTar,
            nextEntry,
            binDir,
            "bin");
        if (!extracted) {
          extracted = maybeCopyTarEntry(
              dockerResourcesTar,
              nextEntry,
              sbinDir,
              "sbin");
        }
        if (!extracted) {
          maybeCopyTarEntry(
              dockerResourcesTar,
              nextEntry,
              entrypointDir,
              "entrypoint");
        }
        nextEntry = dockerResourcesTar.getNextTarEntry();
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static boolean maybeCopyTarEntry(
      TarArchiveInputStream tarInput,
      TarArchiveEntry entry,
      File outputDir,
      String scriptsDir) throws IOException {
    if (entry.isFile() && entry.getName().startsWith(
        String.format("docker-resources%s%s", File.separator, scriptsDir))) {
      String fileName = Paths.get(entry.getName()).toFile().getName();
      File scriptFile = new File(outputDir, fileName);

      try (FileOutputStream scriptFileStream = new FileOutputStream(scriptFile)) {
        IOUtils.copy(tarInput, scriptFileStream);
      }
      return true;
    } else {
      return false;
    }
  }

  private static void deleteAndMkDirs(File dir) {
    if (dir.exists()) {
      try {
        FileUtils.deleteDirectory(dir);
      } catch (IOException e) {
        throw new RuntimeException(
            String.format("Failed to delete previously present scripts directory at %s.",
                dir.getAbsolutePath()));
      }
    }
    if (!dir.mkdirs()) {
      throw new RuntimeException(
          String.format("Failed to create scripts directory.", dir.getAbsolutePath()));
    }
  }
}
