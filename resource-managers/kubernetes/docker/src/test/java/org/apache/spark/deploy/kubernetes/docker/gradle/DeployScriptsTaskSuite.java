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
import java.io.IOException;
import java.nio.file.Paths;

import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

public final class DeployScriptsTaskSuite {

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  private File dockerBuildDir;

  @Before
  public void before() throws IOException {
    dockerBuildDir = tempFolder.newFolder("docker");
  }

  @Test
  public void testDeployScripts_unpacksFilesIntoDockerBuildDir() {
    DeployScriptsTask task = Mockito.mock(DeployScriptsTask.class);
    Mockito.when(task.getDockerBuildDir()).thenReturn(dockerBuildDir);
    Mockito.doCallRealMethod().when(task).deployScripts();
    task.deployScripts();
    File expectedEntrypointFile = dockerBuildDir.toPath().resolve(
        Paths.get("kubernetes", "dockerfiles", "spark", "entrypoint.sh")).toFile();
    File expectedSparkSubmitFile = dockerBuildDir.toPath().resolve(
        Paths.get("bin", "spark-submit")).toFile();
    File expectedStartHistoryServerFile = dockerBuildDir.toPath().resolve(
        Paths.get("sbin", "start-history-server.sh")).toFile();
    Assertions.assertThat(expectedEntrypointFile).isFile();
    Assertions.assertThat(expectedSparkSubmitFile).isFile();
    Assertions.assertThat(expectedSparkSubmitFile).isFile();
  }
}
