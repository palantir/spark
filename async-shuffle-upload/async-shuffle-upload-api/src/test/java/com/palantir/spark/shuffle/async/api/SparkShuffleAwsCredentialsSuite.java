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

package com.palantir.spark.shuffle.async.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.nio.charset.StandardCharsets;
import org.junit.Test;

public final class SparkShuffleAwsCredentialsSuite {

    @Test
    public void testSerialize() {
        SparkShuffleAwsCredentials creds = SparkShuffleAwsCredentials.builder()
                .accessKeyId("access-key")
                .secretAccessKey("secret-key")
                .sessionToken("session-token")
                .build();
        byte[] bytes = creds.toBytes();
        assertThat(new String(bytes, StandardCharsets.UTF_8))
                .isEqualTo("{\"accessKeyId\":\"access-key\","
                        + "\"secretAccessKey\":\"secret-key\","
                        + "\"sessionToken\":\"session-token\"}");
    }

    @Test
    public void testDeserialize() {
        String serializedString = "{\"accessKeyId\":\"access-key\","
                + "\"secretAccessKey\":\"secret-key\","
                + "\"sessionToken\":\"session-token\"}";

        SparkShuffleAwsCredentials creds =
                SparkShuffleAwsCredentials.fromBytes(serializedString.getBytes(StandardCharsets.UTF_8));
    }
}
