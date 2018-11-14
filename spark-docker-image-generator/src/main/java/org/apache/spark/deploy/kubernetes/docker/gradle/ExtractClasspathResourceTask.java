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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.Arrays;

import org.apache.commons.io.IOUtils;
import org.gradle.api.DefaultTask;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.TaskAction;

public class ExtractClasspathResourceTask extends DefaultTask {

  private String resourcePath;
  private File destinationFile;

  public final String getResourcePath() {
    return resourcePath;
  }

  @OutputFile
  public final File getDestinationFile() {
    return destinationFile;
  }

  public final void setResourcePath(String resourcePath) {
    this.resourcePath = resourcePath;
  }

  public final void setDestinationFile(File destinationFile) {
    this.destinationFile = destinationFile;
  }

  @TaskAction
  public final void exec() {
    ByteArrayOutputStream resolvedBytesOutput;
    try (InputStream input = getClass().getClassLoader().getResourceAsStream(getResourcePath());
        ByteArrayOutputStream bytesOutput = new ByteArrayOutputStream()) {
      IOUtils.copy(input, bytesOutput);
      resolvedBytesOutput = bytesOutput;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    byte[] resourceBytes = resolvedBytesOutput.toByteArray();
    if (getDestinationFile().isFile()) {
      byte[] existingDestinationFileBytes;
      try {
        existingDestinationFileBytes = Files.readAllBytes(getDestinationFile().toPath());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      if (Arrays.equals(resourceBytes, existingDestinationFileBytes)) {
        return;
      } else if (!getDestinationFile().delete()) {
        throw new RuntimeException(
                String.format("Failed to delete existing file at %s.", getDestinationFile().getAbsolutePath()));
      }
    }
    try (FileOutputStream output = new FileOutputStream(getDestinationFile())) {
      output.write(resourceBytes);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
