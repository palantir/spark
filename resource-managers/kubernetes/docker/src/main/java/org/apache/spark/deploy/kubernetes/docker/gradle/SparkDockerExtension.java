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

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SparkDockerExtension {

  private String baseImage = "openjdk:8-alpine";
  private String imageName;
  private Set<String> tags = Collections.unmodifiableSet(Collections.emptySet());

  public final String getBaseImage() {
    if (baseImage == null) {
      throw new IllegalArgumentException("Must provide a non-null base image.");
    }
    return baseImage;
  }

  public final String getImageName() {
    if (imageName == null) {
      throw new IllegalArgumentException("Image name prefix cannot be null.");
    }
    return imageName;
  }

  public final Set<String> getTags() {
    return tags;
  }

  @SuppressWarnings("HiddenField")
  public final void baseImage(String baseImage) {
    this.baseImage = baseImage;
  }

  @SuppressWarnings("HiddenField")
  public final void imageName(String imageName){
    this.imageName = imageName;
  }

  @SuppressWarnings("HiddenField")
  public final void tags(List<String> tags) {
    this.tags = tags.stream().collect(Collectors.toSet());
  }

  @SuppressWarnings("HiddenField")
  public final void tags(String... tags) {
    this.tags = Stream.of(tags).collect(Collectors.toSet());
  }
}
