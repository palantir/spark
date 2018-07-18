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
import java.net.URL;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.file.RelativePath;
import org.gradle.api.provider.ListProperty;
import org.gradle.api.provider.Property;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.Copy;
import org.gradle.api.tasks.Exec;
import org.gradle.api.tasks.Sync;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class SparkDockerPlugin implements Plugin<Project> {
  private static final Logger log = LoggerFactory.getLogger(SparkDockerPlugin.class);

  public static final String DOCKER_IMAGE_EXTENSION = "sparkDocker";
  public static final String SPARK_JARS_DIR = "jars";
  public static final String SPARK_DOCKER_RUNTIME_CONFIGURATION_NAME = "sparkDockerRuntime";

  @Override
  public void apply(Project project) {
    File dockerBuildDirectory = new File(project.getBuildDir(), "spark-docker-build");
    SparkDockerExtension extension = project.getExtensions().create(
        DOCKER_IMAGE_EXTENSION, SparkDockerExtension.class);
    if (!dockerBuildDirectory.isDirectory() && !dockerBuildDirectory.mkdirs()) {
      throw new RuntimeException("Failed to create Docker build directory at "
          + dockerBuildDirectory.getAbsolutePath());
    }
    Configuration sparkDockerRuntimeConfiguration =
        project.getConfigurations().maybeCreate(SPARK_DOCKER_RUNTIME_CONFIGURATION_NAME);
    File dockerFile = new File(dockerBuildDirectory, "Dockerfile");
    project.getPluginManager().withPlugin("java", plugin -> {
      Configuration runtimeConfiguration = project.getConfigurations().findByName("runtime");
      if (runtimeConfiguration == null) {
        log.warn("No runtime configuration was found for a reference configuration for building"
            + " your Spark application's docker images.");
      } else {
        sparkDockerRuntimeConfiguration.extendsFrom(runtimeConfiguration);
      }
      Task jarTask = project.getTasks().getByName("jar");
      Provider<File> sparkAppJar = project.getProviders().provider(() ->
        jarTask.getOutputs().getFiles().getSingleFile());
      Provider<File> jarsDirProvider = project.getProviders().provider(() ->
          new File(dockerBuildDirectory, SPARK_JARS_DIR));
      Task copySparkAppLibTask = project.getTasks().create(
          "copySparkAppLibIntoDocker",
          Copy.class,
          copyTask -> copyTask.from(
              project.files(project.getConfigurations().getByName(SPARK_DOCKER_RUNTIME_CONFIGURATION_NAME)),
              sparkAppJar)
              .into(jarsDirProvider));
      copySparkAppLibTask.dependsOn(jarTask);
      URL dockerResourcesUrl = getClass().getResource("/docker-resources");
      Sync deployScriptsTask = project.getTasks().create(
          "sparkDockerDeployScripts", Sync.class, task -> {
            task.from(project.fileTree(dockerResourcesUrl));
            task.setIncludeEmptyDirs(false);
            task.into(dockerBuildDirectory);
          });
      copySparkAppLibTask.dependsOn(deployScriptsTask);
      GenerateDockerFileTask generateDockerFileTask = project.getTasks().create(
          "sparkDockerGenerateDockerFile", GenerateDockerFileTask.class);
      generateDockerFileTask.setSrcDockerFile(
          Paths.get(
              dockerBuildDirectory.getAbsolutePath(),
              "kubernetes",
              "dockerfiles",
              "spark",
              "Dockerfile.original").toFile());
      generateDockerFileTask.setDestDockerFile(dockerFile);
      generateDockerFileTask.dependsOn(deployScriptsTask);
      Property<String> baseImageProperty = project.getObjects().property(String.class);
      baseImageProperty.set(project.getProviders().provider(extension::getBaseImage));
      generateDockerFileTask.setBaseImage(baseImageProperty);
      Task prepareTask = project.getTasks().create("sparkDockerPrepare");
      prepareTask.dependsOn(
          deployScriptsTask, generateDockerFileTask, "copySparkAppLibIntoDocker");
    });
    setupDockerTasks(dockerBuildDirectory, dockerFile, project, extension);
  }

  private void setupDockerTasks(
      File buildDirectory,
      File dockerFile,
      Project project,
      SparkDockerExtension extension) {
    Property<String> imageNameProperty = project.getObjects().property(String.class);
    imageNameProperty.set(project.provider(extension::getImageName));
    DockerBuildTask dockerBuild = project.getTasks().create(
        "sparkDockerBuild",
        DockerBuildTask.class,
        dockerBuildTask -> {
          dockerBuildTask.setDockerBuildDirectory(buildDirectory);
          dockerBuildTask.setDockerFile(dockerFile);
          dockerBuildTask.setImageName(imageNameProperty);
          dockerBuildTask.dependsOn("sparkDockerPrepare");
        });
    Task tagAllTask = project.getTasks().create("sparkDockerTag");
    LazyExecTask pushAllTask = project.getTasks().create(
        "sparkDockerPush",
        LazyExecTask.class,
        task -> {
          ListProperty<String> pushCommandLine = project.getObjects().listProperty(String.class);
          pushCommandLine.set(project.provider(() -> {
            List<String> commandLine = new ArrayList<>();
            commandLine.add("docker");
            commandLine.add("push");
            commandLine.add(extension.getImageName());
            return commandLine;
          }));
          task.setCommandLine(pushCommandLine);
        });
    project.afterEvaluate(evaluatedProject -> {
      List<Exec> tagTasks = extension.getTags().stream()
          .map(tag ->
              evaluatedProject
                  .getTasks()
                  .create(
                      String.format("sparkDockerTag%s", tag),
                      Exec.class,
                      task ->
                          task.commandLine(
                              "docker",
                              "tag",
                              extension.getImageName(),
                              String.format("%s:%s", extension.getImageName(), tag))
                              .dependsOn(dockerBuild)))
          .collect(Collectors.toList());
      if (!tagTasks.isEmpty()) {
        tagAllTask.dependsOn(tagTasks);
      } else {
        tagAllTask.dependsOn(dockerBuild);
      }
      List<Exec> pushTasks = extension.getTags().stream()
          .map(tag ->
              evaluatedProject.getTasks().create(
                  String.format("sparkDockerPush%s", tag),
                  Exec.class,
                  task ->
                      task.commandLine(
                          "docker", "push", String.format("%s:%s", extension.getImageName(), tag))
                          .dependsOn("sparkDockerTag%s")))
          .collect(Collectors.toList());
      if (!pushTasks.isEmpty()) {
        pushAllTask.dependsOn(pushTasks);
      } else {
        pushAllTask.dependsOn(tagAllTask);
      }
    });
  }
}
