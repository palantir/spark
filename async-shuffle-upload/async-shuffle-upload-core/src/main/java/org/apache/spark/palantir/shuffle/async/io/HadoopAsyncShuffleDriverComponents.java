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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.palantir.logsafe.SafeArg;

import java.io.IOException;
import java.security.KeyPair;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import org.apache.spark.SparkEnv;
import org.apache.spark.palantir.shuffle.async.AsyncShuffleDataIoSparkConfigs;
import org.apache.spark.palantir.shuffle.async.AsyncShuffleUploadDriverEndpoint;
import org.apache.spark.palantir.shuffle.async.JavaSparkConf;
import org.apache.spark.palantir.shuffle.async.metadata.MapOutputId;
import org.apache.spark.palantir.shuffle.async.metadata.ShuffleStorageStateTracker;
import org.apache.spark.palantir.shuffle.async.metadata.ShuffleStorageStateVisitor;
import org.apache.spark.palantir.shuffle.async.util.keys.KeyPairExtraConfigKeys;
import org.apache.spark.palantir.shuffle.async.util.keys.KeyPairs;
import org.apache.spark.rpc.RpcEndpoint;
import org.apache.spark.rpc.RpcEnv;
import org.apache.spark.shuffle.api.ShuffleDriverComponents;
import org.apache.spark.storage.BlockManagerId;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class HadoopAsyncShuffleDriverComponents implements ShuffleDriverComponents {

  private static final Logger LOG =
      LoggerFactory.getLogger(HadoopAsyncShuffleDriverComponents.class);

  private final ShuffleDriverComponents delegate;
  private final ShuffleStorageStateTracker shuffleStorageStateTracker;
  private final Supplier<SparkEnv> sparkEnvSupplier;

  private RpcEndpoint shuffleUploadDriverEndpoint;

  public HadoopAsyncShuffleDriverComponents(
      ShuffleDriverComponents delegate,
      ShuffleStorageStateTracker shuffleStorageStateTracker) {
    this(delegate, shuffleStorageStateTracker, SparkEnv::get);
  }

  @VisibleForTesting
  HadoopAsyncShuffleDriverComponents(
      ShuffleDriverComponents delegate,
      ShuffleStorageStateTracker shuffleStorageStateTracker,
      Supplier<SparkEnv> sparkEnvSupplier) {
    this.delegate = delegate;
    this.shuffleStorageStateTracker = shuffleStorageStateTracker;
    this.sparkEnvSupplier = sparkEnvSupplier;
  }

  @Override
  public Map<String, String> initializeApplication() {
    SparkEnv sparkEnv = sparkEnvSupplier.get();
    RpcEnv rpcEnv = sparkEnv.rpcEnv();
    JavaSparkConf javaSparkConf = new JavaSparkConf(sparkEnv.conf());

    shuffleUploadDriverEndpoint = AsyncShuffleUploadDriverEndpoint.create(
        rpcEnv,
        shuffleStorageStateTracker);
    rpcEnv.setupEndpoint(AsyncShuffleUploadDriverEndpoint.NAME(), shuffleUploadDriverEndpoint);

    ImmutableMap.Builder<String, String> extraConfigsBuilder = ImmutableMap.builder();
    extraConfigsBuilder.putAll(delegate.initializeApplication());

    if (javaSparkConf.getBoolean(AsyncShuffleDataIoSparkConfigs.ENCRYPTION_ENABLED())) {
      KeyPair keyPair = KeyPairs.generateKeyPair(
          javaSparkConf.get(AsyncShuffleDataIoSparkConfigs.ENCRYPTION_KEY_ALGORITHM()),
          javaSparkConf.getInt(AsyncShuffleDataIoSparkConfigs.ENCRYPTION_KEY_SIZE())
      );

      extraConfigsBuilder.put(
          KeyPairExtraConfigKeys.PUBLIC_KEY, KeyPairs.serializeKey(keyPair.getPublic()));
      extraConfigsBuilder.put(
          KeyPairExtraConfigKeys.PRIVATE_KEY, KeyPairs.serializeKey(keyPair.getPrivate()));
    }

    return extraConfigsBuilder.build();
  }

  /**
   * Called when the application is shutting down.
   * <p>
   * For now, we don't clear all the data on the remote storage layer. This is because the removal
   * operation may be prohibitively expensive, particularly in the case of S3.
   */
  @Override
  public void cleanupApplication() throws IOException {
    LOG.info("Cleaning up application data");
    shuffleUploadDriverEndpoint.stop();
    delegate.cleanupApplication();
  }

  @Override
  public void registerShuffle(int shuffleId) throws IOException {
    shuffleStorageStateTracker.registerShuffle(shuffleId);
    delegate.registerShuffle(shuffleId);
  }

  @Override
  public void removeShuffle(int shuffleId, boolean blocking) throws IOException {
    LOG.info("Cleaning up shuffle data ", SafeArg.of("shuffleId", shuffleId));
    shuffleStorageStateTracker.unregisterShuffle(shuffleId);
    delegate.removeShuffle(shuffleId, blocking);
  }

  /**
   * Called by the {@link org.apache.spark.MapOutputTracker} to determine if a block should be
   * re-computed by a retried task.
   * <p>
   * The implementation will report that the block does not need to be recomputed if it can be
   * fetched from remote storage.
   */
  @Override
  public boolean checkIfMapOutputStoredOutsideExecutor(
      int shuffleId, int mapId, long mapTaskAttemptId) {
    return shuffleStorageStateTracker.getShuffleStorageState(
        new MapOutputId(shuffleId, mapId, mapTaskAttemptId))
        .visit(new ShuffleStorageStateVisitor<Boolean>() {
          @Override
          public Boolean unregistered() {
            return false;
          }

          @Override
          public Boolean onExecutorOnly(BlockManagerId _executorLocation) {
            return false;
          }

          @Override
          public Boolean onExecutorAndRemote(
              BlockManagerId _executorLocation, Optional<Long> _mergeId) {
            return true;
          }

          @Override
          public Boolean onRemoteOnly(Optional<Long> _mergeId) {
            return true;
          }
        });
  }

  @Override
  public boolean unregisterOutputOnHostOnFetchFailure() {
    return delegate.unregisterOutputOnHostOnFetchFailure();
  }
}
