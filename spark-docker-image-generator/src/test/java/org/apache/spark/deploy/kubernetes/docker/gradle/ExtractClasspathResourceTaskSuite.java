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
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import com.google.common.io.Files;
import org.apache.commons.io.IOUtils;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public final class ExtractClasspathResourceTaskSuite {

  private static final String TEST_RESOURCE_LOCATION =
      "META-INF/gradle-plugins/org.apache.spark.deploy.kubernetes.docker.gradle.properties";

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Mock
  private ExtractClasspathResourceTask taskUnderTest;

  private byte[] expectedResourceFileBytes;

  @Before
  public void before() throws IOException {
    MockitoAnnotations.initMocks(this);
    ByteArrayOutputStream resolvedBytesOut;
    try (InputStream expectedInput = getClass().getClassLoader().getResourceAsStream(TEST_RESOURCE_LOCATION);
        ByteArrayOutputStream bytesOut = new ByteArrayOutputStream()) {
      if (expectedInput == null) {
        throw new NullPointerException("Resource stream for test was not found.");
      }
      IOUtils.copy(expectedInput, bytesOut);
      resolvedBytesOut = bytesOut;
    }
    expectedResourceFileBytes = resolvedBytesOut.toByteArray();
  }

  @Test
  public void testExtractingToNewFile() throws IOException {
    File resourceFileDir = tempFolder.newFolder();
    File resourceFile = new File(resourceFileDir, "test-resource");
    resourceFile.getParentFile().mkdirs();
    taskUnderTest.setDestinationFile(resourceFile);
    taskUnderTest.setResourcePath(TEST_RESOURCE_LOCATION);
    Assertions.assertThat(resourceFile).doesNotExist();
    taskUnderTest.exec();
    Assertions.assertThat(resourceFile).exists();
    Assertions.assertThat(resourceFile).hasBinaryContent(expectedResourceFileBytes);
  }

  @Test
  public void testExtractingToExistingFile() throws IOException {
    File resourceFile = tempFolder.newFile("test-resource");
    Files.write("some-old-contents", resourceFile, StandardCharsets.UTF_8);
    taskUnderTest.setDestinationFile(resourceFile);
    taskUnderTest.setResourcePath(TEST_RESOURCE_LOCATION);
    Assertions.assertThat(resourceFile).exists();
    taskUnderTest.exec();
    Assertions.assertThat(resourceFile).exists();
    Assertions.assertThat(resourceFile).hasBinaryContent(expectedResourceFileBytes);
  }
}
