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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import com.palantir.spark.shuffle.async.immutables.ImmutablesStyle;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.immutables.value.Value;

@ImmutablesStyle
@Value.Immutable
@JsonSerialize(as = ImmutableSparkShuffleAwsCredentials.class)
@JsonDeserialize(as = ImmutableSparkShuffleAwsCredentials.class)
@JsonIgnoreProperties(ignoreUnknown = true)
public abstract class SparkShuffleAwsCredentials {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public abstract String accessKeyId();

    public abstract String secretAccessKey();

    public abstract String sessionToken();

    public final byte[] toBytes() {
        try {
            return MAPPER.writeValueAsString(this).getBytes(StandardCharsets.UTF_8);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public static SparkShuffleAwsCredentials fromBytes(byte[] bytes) {
        try {
            return MAPPER.readValue(new String(bytes, StandardCharsets.UTF_8), SparkShuffleAwsCredentials.class);
        } catch (IOException e) {
            throw new SafeIllegalArgumentException(
                "Could not deserialize bytes as AWS credentials.",
                UnsafeArg.of("cause", e));
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder extends ImmutableSparkShuffleAwsCredentials.Builder {}
}
