/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.deploy.kubernetes.docker.gradle;

import com.palantir.sls.versions.OrderableSlsVersion;
import com.palantir.sls.versions.SlsVersionType;
import org.gradle.api.Project;

public final class ImageResolver {

    public static String resolveImageName(
            Project project,
            String imagePath,
            String snapshotRegistry,
            String releaseRegistry) {
        StringBuilder imageNameBuilder = new StringBuilder();
        if (isReleaseVersion(project.getVersion().toString())) {
            imageNameBuilder.append(releaseRegistry);
        } else {
            imageNameBuilder.append(snapshotRegistry);
        }
        if (!imagePath.startsWith("/")) {
            imageNameBuilder.append('/');
        }
        imageNameBuilder.append(imagePath);
        return imageNameBuilder.toString();
    }

    private static boolean isReleaseVersion(String versionString) {
        return OrderableSlsVersion.safeValueOf(versionString)
                .map(version -> version.getType() == SlsVersionType.RELEASE
                        || version.getType() == SlsVersionType.RELEASE_CANDIDATE)
                .orElse(false);
    }

    private ImageResolver() {}
}
