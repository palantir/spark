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

package org.apache.spark.palantir.shuffle.async.io;

import com.palantir.crypto2.keys.KeyPairs;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.palantir.shuffle.async.AsyncShuffleDataIoSparkConfigs;
import org.apache.spark.palantir.shuffle.async.JavaSparkConf;
import org.apache.spark.palantir.shuffle.async.metadata.ShuffleStorageStateTracker;
import org.apache.spark.palantir.shuffle.async.util.keys.KeyPairExtraConfigKeys;
import org.apache.spark.rpc.RpcEnv;
import org.apache.spark.shuffle.api.ShuffleDriverComponents;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.security.KeyPair;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

public final class HadoopAsyncShuffleDriverComponentsTest {
  @Mock
  private SparkEnv sparkEnv;

  @Mock
  private RpcEnv rpcEnv;

  @Mock
  private ShuffleDriverComponents delegate;

  @Mock
  private ShuffleStorageStateTracker shuffleStorageStateTracker;

  @Before
  public void before() throws IOException {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testGenerateKeyPair() throws IOException {
    SparkConf sparkConf = new SparkConf()
        .set(AsyncShuffleDataIoSparkConfigs.ENCRYPTION_ENABLED(), "true");
    JavaSparkConf javaSparkConf = new JavaSparkConf(sparkConf);
    when(sparkEnv.rpcEnv()).thenReturn(rpcEnv);
    when(sparkEnv.conf()).thenReturn(sparkConf);

    HadoopAsyncShuffleDriverComponents hadoopAsyncShuffleDriverComponents =
        new HadoopAsyncShuffleDriverComponents(
            delegate, shuffleStorageStateTracker, () -> sparkEnv);

    Map<String, String> extraConfigs = hadoopAsyncShuffleDriverComponents.initializeApplication();

    assertThat(extraConfigs).containsKeys(
        KeyPairExtraConfigKeys.PRIVATE_KEY,
        KeyPairExtraConfigKeys.PUBLIC_KEY
    );

    // make sure the generated key deserializes properly
    KeyPair keyPair = KeyPairs.fromStrings(
        extraConfigs.get(KeyPairExtraConfigKeys.PRIVATE_KEY),
        extraConfigs.get(KeyPairExtraConfigKeys.PUBLIC_KEY),
        javaSparkConf.get(AsyncShuffleDataIoSparkConfigs.ENCRYPTION_KEY_ALGORITHM())
    );

    assertThat(keyPair).isNotNull();
  }
}
