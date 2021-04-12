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

import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.gradle.api.Project;
import org.gradle.api.provider.Property;
import org.gradle.api.provider.SetProperty;

public class SparkDockerExtension {

    private final Property<String> baseImage;
    private final Property<String> imagePath;
    private final Property<String> snapshotRegistry;
    private final Property<String> releaseRegistry;
    private final SetProperty<String> tags;
    private final Property<String> resolvedImage;

    public SparkDockerExtension(Project project) {
        this.baseImage = project.getObjects().property(String.class);
        this.imagePath = project.getObjects().property(String.class);
        this.snapshotRegistry = project.getObjects().property(String.class);
        this.releaseRegistry = project.getObjects().property(String.class);
        this.tags = project.getObjects().setProperty(String.class);
        this.resolvedImage = project.getObjects().property(String.class);
        resolvedImage.set(project.provider(() ->
                ImageResolver.resolveImageName(
                        project,
                        imagePath.get(),
                        snapshotRegistry.get(),
                        releaseRegistry.get())));
    }

    public final Property<String> getBaseImage() {
        return baseImage;
    }

    public final Property<String> getImagePath() {
        return imagePath;
    }

    public final Property<String> getSnapshotRegistry() {
        return snapshotRegistry;
    }

    public final Property<String> getReleaseRegistry() {
        return releaseRegistry;
    }

    public final SetProperty<String> getTags() {
        return tags;
    }

    public final Property<String> getResolvedImage() {
        return resolvedImage;
    }

    @SuppressWarnings("HiddenField")
    public final void baseImage(String baseImage) {
        this.baseImage.set(baseImage);
    }

    @SuppressWarnings("HiddenField")
    public final void imagePath(String imagePath) {
        this.imagePath.set(imagePath);
    }

    @SuppressWarnings("HiddenField")
    public final void snapshotRegistry(String snapshotRegistry) {
        this.snapshotRegistry.set(snapshotRegistry);
    }

    @SuppressWarnings("HiddenField")
    public final void releaseRegistry(String releaseRegistry) {
        this.releaseRegistry.set(releaseRegistry);
    }

    @SuppressWarnings("HiddenField")
    public final void tags(Collection<String> tags) {
        this.tags.set(tags);
    }

    @SuppressWarnings("HiddenField")
    public final void tags(String... tags) {
        this.tags.set(Stream.of(tags).collect(Collectors.toSet()));
    }
}
