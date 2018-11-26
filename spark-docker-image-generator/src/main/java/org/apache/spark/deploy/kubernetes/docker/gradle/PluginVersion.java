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
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

import org.apache.commons.io.IOUtils;

public final class PluginVersion {

  public static String get() {
    String pluginVersion = Optional.ofNullable(PluginVersion.class.getPackage().getImplementationVersion())
        .orElseGet(() -> {
          ByteArrayOutputStream versionBytes = new ByteArrayOutputStream();
          try (InputStream versionFileInputStream =
              PluginVersion.class.getResourceAsStream("/version/version.txt")) {
            IOUtils.copy(versionFileInputStream, versionBytes);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
          return new String(versionBytes.toByteArray(), StandardCharsets.UTF_8);
        });
    if (pluginVersion == null) {
      throw new RuntimeException("Version could not be determined for the Gradle plugin.");
    }
    return pluginVersion;
  }

  private PluginVersion() {}
}
