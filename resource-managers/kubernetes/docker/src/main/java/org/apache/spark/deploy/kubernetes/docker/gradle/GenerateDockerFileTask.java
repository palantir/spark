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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.gradle.api.DefaultTask;
import org.gradle.api.provider.Property;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.TaskAction;

public class GenerateDockerFileTask extends DefaultTask {

  private File destDockerFile;
  private Property<String> baseImage;

  public void setDestDockerFile(File destDockerFile) {
    this.destDockerFile = destDockerFile;
  }

  public void setBaseImage(Property<String> baseImage) {
    this.baseImage = baseImage;
  }

  @OutputFile
  public File getDestDockerFile() {
    return destDockerFile;
  }

  @Input
  public Property<String> getBaseImage() {
    return baseImage;
  }

  @TaskAction
  public void generateDockerFile() {
    File currentDestDockerFile = getDestDockerFile();
    try (InputStream dockerResourcesInputStream =
             getClass().getResourceAsStream("/docker-resources.tgz");
         GZIPInputStream dockerResourcesGZipped = new GZIPInputStream(dockerResourcesInputStream);
         TarArchiveInputStream dockerResourcesTar =
             new TarArchiveInputStream(dockerResourcesGZipped)) {
      TarArchiveEntry nextEntry = dockerResourcesTar.getNextTarEntry();
      while (nextEntry != null) {
        String fileName = Paths.get(nextEntry.getName()).toFile().getName();
        if (fileName.equals("Dockerfile")) {
          try (InputStreamReader sourceDockerFileReader =
                   new InputStreamReader(dockerResourcesTar, StandardCharsets.UTF_8);
               BufferedReader sourceDockerFileBuffered =
                   new BufferedReader(sourceDockerFileReader)) {
            List<String> fileLines = Collections.unmodifiableList(
                sourceDockerFileBuffered.lines()
                    .filter(line ->
                        !line.equals("COPY examples /opt/spark/examples")
                            && !line.equals("COPY data /opt/spark/data"))
                    .map(line -> {
                      if (line.equals("FROM openjdk:8-alpine")) {
                        return String.format("FROM %s", getBaseImage().get());
                      } else {
                        return line;
                      }
                    }).collect(Collectors.toList()));
            Files.write(currentDestDockerFile.toPath(), fileLines, StandardCharsets.UTF_8);
          }
          break;
        }
        nextEntry = dockerResourcesTar.getNextTarEntry();
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
