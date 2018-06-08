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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.tasks.Copy;
import org.gradle.api.tasks.Exec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class SparkDockerPlugin implements Plugin<Project> {
  private static final Logger log = LoggerFactory.getLogger(SparkDockerPlugin.class);

  public static final String DOCKER_IMAGE_EXTENSION = "sparkDocker";
  public static final String SPARK_JARS_DIR = "jars";
  public static final String SPARK_DOCKER_RUNTIME_CONFIGURATION_NAME = "sparkDockerRuntime";

  @Override
  public void apply(Project project) {
    project.getPluginManager().apply(getClass());
    project.getExtensions().create(DOCKER_IMAGE_EXTENSION, SparkDockerExtension.class);
    File dockerBuildDirectory = new File(project.getBuildDir(), "spark-docker-build");
    Configuration sparkDockerRuntimeConfiguration =
        project.getConfigurations().maybeCreate(SPARK_DOCKER_RUNTIME_CONFIGURATION_NAME);
    if (!dockerBuildDirectory.isDirectory() && !dockerBuildDirectory.mkdirs()) {
      throw new RuntimeException("Failed to create Docker build directory at "
          + dockerBuildDirectory.getAbsolutePath());
    }

    project.afterEvaluate(evaluatedProject -> {
      Configuration runtimeConfiguration = project.getConfigurations().findByName("runtime");
      if (runtimeConfiguration == null) {
        log.warn("No runtime configuration was found for a reference configuration for building"
            + " your Spark application's docker images.");
      } else {
        sparkDockerRuntimeConfiguration.extendsFrom(runtimeConfiguration);
      }

      Task jarTask = project.getTasks().getByName("jar");
      File sparkAppJar = jarTask.getOutputs().getFiles().getSingleFile();
      Task copySparkAppLibTask = project.getTasks().create("copySparkAppLibIntoDocker", Copy.class,
          copyTask -> copyTask.from(
              project.files(
                  project.getConfigurations().getByName(SPARK_DOCKER_RUNTIME_CONFIGURATION_NAME)),
              sparkAppJar)
              .into(new File(dockerBuildDirectory, SPARK_JARS_DIR)));
      copySparkAppLibTask.setDependsOn(Collections.singletonList(jarTask));

      DeployScriptsTask deployScriptsTask = project.getTasks().create(
          "sparkDockerDeployScripts", DeployScriptsTask.class);
      deployScriptsTask.setDockerBuildDir(dockerBuildDirectory);
      GenerateDockerFileTask generateDockerFileTask = project.getTasks().create(
          "sparkDockerGenerateDockerFile", GenerateDockerFileTask.class);
      Task prepareTask = project.getTasks().create("sparkDockerPrepare");
      prepareTask.setDependsOn(Arrays.asList(
          deployScriptsTask, generateDockerFileTask, copySparkAppLibTask));
      File dockerFile = new File(dockerBuildDirectory, "Dockerfile");
      generateDockerFileTask.setDestDockerFile(dockerFile);
      SparkDockerExtension extension = evaluatedProject
          .getExtensions().getByType(SparkDockerExtension.class);
      generateDockerFileTask.setBaseImage(extension.getBaseImage());
      setupDockerTasks(dockerBuildDirectory, dockerFile, evaluatedProject, extension);
    });
  }

  private void setupDockerTasks(
      File buildDirectory,
      File dockerFile,
      Project evaluatedProject,
      SparkDockerExtension extension) {
    Exec dockerBuild = evaluatedProject.getTasks().create(
        "sparkDockerBuild", Exec.class);
    dockerBuild.setCommandLine(
        "docker",
        "build",
        "-f",
        dockerFile.getAbsolutePath(),
        "-t",
        extension.getImageName(),
        buildDirectory.getAbsolutePath());
    dockerBuild.setDependsOn(
        Arrays.asList(evaluatedProject.getTasks().getByName("sparkDockerPrepare")));
    List<Exec> tagTasks = extension.getTags().stream()
        .map(tag -> {
          Exec tagTask = evaluatedProject
              .getTasks()
              .create(String.format("sparkDockerTag%s", tag), Exec.class);
          tagTask.setCommandLine(
              "docker",
              "tag",
              extension.getImageName(),
              String.format("%s:%s", extension.getImageName(), tag));
          tagTask.setDependsOn(Arrays.asList(dockerBuild));
          return tagTask;
        }).collect(Collectors.toList());
    Task tagAllTask = evaluatedProject.getTasks().create("sparkDockerTag");
    if (!tagTasks.isEmpty()) {
      tagAllTask.setDependsOn(tagTasks);
    } else {
      tagAllTask.setDependsOn(Arrays.asList(dockerBuild));
    }
    Exec pushAllTask = evaluatedProject.getTasks().create("sparkDockerPush", Exec.class);
    pushAllTask.setCommandLine("docker", "push", extension.getImageName());
    List<Exec> pushTasks = extension.getTags().stream()
        .map(tag -> {
          Exec pushForTagTask = evaluatedProject.getTasks().create(
              String.format("sparkDockerPush%s", tag), Exec.class);
          pushForTagTask.setCommandLine("" +
              "docker", "push", String.format("%s:%s", extension.getImageName(), tag));
          pushForTagTask.setDependsOn(
              Arrays.asList(evaluatedProject.getTasks().getByName(
                  String.format("sparkDockerTag%s", tag))));
          return pushForTagTask;
        }).collect(Collectors.toList());
    if (!pushTasks.isEmpty()) {
      pushAllTask.setDependsOn(pushTasks);
    } else {
      pushAllTask.setDependsOn(Arrays.asList(tagAllTask));
    }
  }
}
