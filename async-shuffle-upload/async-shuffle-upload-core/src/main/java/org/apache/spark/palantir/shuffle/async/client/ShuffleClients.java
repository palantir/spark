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

package org.apache.spark.palantir.shuffle.async.client;

import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.crypto2.hadoop.EncryptedFileSystem;
import com.palantir.crypto2.hadoop.FileKeyStorageStrategy;
import com.palantir.crypto2.keys.KeyPairs;
import com.palantir.crypto2.keys.KeyStorageStrategy;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;

import java.io.IOException;
import java.net.URI;
import java.security.KeyPair;
import java.time.Clock;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.hadoop.fs.FileSystem;

import org.apache.spark.SparkConf;
import org.apache.spark.palantir.shuffle.async.ShuffleDriverEndpointRef;
import org.apache.spark.palantir.shuffle.async.client.basic.DefaultPartitionOffsetsFetcher;
import org.apache.spark.palantir.shuffle.async.client.basic.DefaultRemoteShuffleFileSystem;
import org.apache.spark.palantir.shuffle.async.client.basic.HadoopShuffleClient;
import org.apache.spark.palantir.shuffle.async.client.basic.LocalDownloadShuffleFileSystem;
import org.apache.spark.palantir.shuffle.async.client.basic.PartitionOffsetsFetcher;
import org.apache.spark.palantir.shuffle.async.client.basic.RemoteShuffleFileSystem;
import org.apache.spark.palantir.shuffle.async.client.merging.DefaultMergingShuffleFiles;
import org.apache.spark.palantir.shuffle.async.client.merging.DefaultShuffleFileBatchUploader;
import org.apache.spark.palantir.shuffle.async.client.merging.MergingHadoopShuffleClientConfiguration;
import org.apache.spark.palantir.shuffle.async.client.merging.MergingShuffleFiles;
import org.apache.spark.palantir.shuffle.async.client.merging.MergingShuffleUploadCoordinator;
import org.apache.spark.palantir.shuffle.async.client.merging.ShuffleFileBatchUploader;
import org.apache.spark.palantir.shuffle.async.metrics.HadoopAsyncShuffleMetrics;
import org.apache.spark.palantir.shuffle.async.util.DaemonExecutors;
import org.apache.spark.palantir.shuffle.async.util.keys.KeyPairExtraConfigKeys;
import org.apache.spark.palantir.shuffle.async.util.NamedExecutors;
import org.apache.spark.util.Utils;
import org.immutables.builder.Builder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ShuffleClients {

  private static final Logger LOGGER = LoggerFactory.getLogger(ShuffleClients.class);

  @Builder.Factory
  static Optional<ShuffleClient> shuffleClient(
      String appId,
      SparkConf sparkConf,
      Map<String, String> extraConfigs,
      ShuffleDriverEndpointRef driverEndpointRef,
      Clock clock,
      Optional<ExecutorService> customUploadExecutorService,
      Optional<ExecutorService> customDownloadExecutorService,
      Optional<ScheduledExecutorService> customUploadCoordinatorExecutorService,
      HadoopAsyncShuffleMetrics metrics) {
    BaseHadoopShuffleClientConfiguration baseConfig =
        BaseHadoopShuffleClientConfiguration.of(sparkConf);
    if (!baseConfig.baseUri().isPresent()) {
      LOGGER.warn("Spark application configured to use the async shuffle upload plugin, but no" +
          " remote storage location was provided for backing up data. Falling back to only" +
          " reading and writing shuffle files from local disk.");
    }
    return baseConfig.baseUri().map(baseUri -> {
      LOGGER.info("Setting up shuffle hadoop client.", UnsafeArg.of("baseUri", baseUri));
      ExecutorService resolvedUploadExecutor = resolveUploadExecutor(
          customUploadExecutorService, baseConfig);
      ExecutorService resolvedDownloadExecutor =
          resolveDownloadExecutor(customDownloadExecutorService, baseConfig);
      FileSystem backupFs = createBackupFs(baseConfig, baseUri, extraConfigs);

      ShuffleStorageStrategy storageStrategy = baseConfig.storageStrategy();
      switch (storageStrategy) {
        case BASIC:
          String localDir = Utils.getLocalDir(sparkConf);
          LocalDownloadShuffleFileSystem localDownloadFs =
              LocalDownloadShuffleFileSystem.createWithLocalDir(localDir);
          RemoteShuffleFileSystem remoteFs = new DefaultRemoteShuffleFileSystem(
              backupFs,
              baseUri.toString(),
              appId);
          PartitionOffsetsFetcher partitionOffsetsFetcher = new DefaultPartitionOffsetsFetcher(
              localDownloadFs,
              remoteFs,
              NamedExecutors.newFixedThreadScheduledExecutorService(1, "cleanup-index-files"),
              driverEndpointRef,
              baseConfig.localFileBufferSize(),
              baseConfig.downloadShuffleBlockBufferSize(),
              baseConfig.cacheIndexFilesLocally());
          return new HadoopShuffleClient(
              appId,
              MoreExecutors.listeningDecorator(resolvedUploadExecutor),
              MoreExecutors.listeningDecorator(resolvedDownloadExecutor),
              remoteFs,
              localDownloadFs,
              partitionOffsetsFetcher,
              metrics.basicShuffleClientMetrics(),
              driverEndpointRef,
              clock,
              baseConfig.downloadShuffleBlockInMemoryMaxSize(),
              baseConfig.downloadShuffleBlockBufferSize(),
              baseConfig.localFileBufferSize());
        case MERGING:
          MergingHadoopShuffleClientConfiguration mergingConf =
              MergingHadoopShuffleClientConfiguration.of(baseConfig);
          MergingShuffleFiles mergingShuffleFiles = DefaultMergingShuffleFiles.mergingShuffleFiles(
              appId,
              baseConfig.localFileBufferSize(),
              baseUri,
              backupFs);

          ScheduledExecutorService resolvedUploadCoordinatorExecutor =
              customUploadCoordinatorExecutorService.orElseGet(() ->
                  NamedExecutors.newFixedThreadScheduledExecutorService(
                      1,
                      "upload-coordinator-%d"));
          ShuffleFileBatchUploader batchUploader = new DefaultShuffleFileBatchUploader(
              appId,
              driverEndpointRef,
              mergingShuffleFiles,
              MoreExecutors.listeningDecorator(resolvedUploadExecutor),
              metrics.mergingShuffleClientMetrics(),
              clock);

          MergingShuffleUploadCoordinator coordinator = new MergingShuffleUploadCoordinator(
              mergingConf.maxBatchSizeBytes(),
              mergingConf.maxBatchAgeMillis(),
              mergingConf.maxBufferedInputs(),
              mergingConf.pollingIntervalMillis(),
              driverEndpointRef,
              MoreExecutors.listeningDecorator(resolvedUploadCoordinatorExecutor),
              clock,
              batchUploader);

          ExecutorService localReadExecService = DaemonExecutors.newFixedDaemonThreadPool(
              mergingConf.readLocalDiskParallelism());
          ShuffleClient client = new MergingHadoopShuffleClient(
              appId,
              MoreExecutors.listeningDecorator(resolvedDownloadExecutor),
              MoreExecutors.listeningDecorator(localReadExecService),
              metrics.mergingShuffleClientMetrics(),
              driverEndpointRef,
              coordinator,
              mergingShuffleFiles,
              clock,
              baseConfig.localFileBufferSize());

          coordinator.start();
          return client;
        default:
          throw new SafeIllegalArgumentException(
              "Storage strategy is invalid.",
              SafeArg.of("invalidStorageStrategy", storageStrategy));
      }
    });
  }

  private static FileSystem createBackupFs(
      BaseHadoopShuffleClientConfiguration baseConfig,
      URI baseUri,
      Map<String, String> extraConfigs) {
    FileSystem backupFs;
    try {
      backupFs = FileSystem.get(baseUri, baseConfig.hadoopConf());
    } catch (IOException e) {
      LOGGER.error("Failed to create filesystem", e);
      throw new RuntimeException(e);
    }

    if (baseConfig.encryptionEnabled()) {
      KeyPair keyPair = KeyPairs.fromStrings(
          extraConfigs.get(KeyPairExtraConfigKeys.PRIVATE_KEY),
          extraConfigs.get(KeyPairExtraConfigKeys.PUBLIC_KEY),
          baseConfig.encryptionKeyAlgorithm()
      );

      KeyStorageStrategy keyStorageStrategy = new FileKeyStorageStrategy(backupFs, keyPair);
      return new EncryptedFileSystem(backupFs, keyStorageStrategy);
    }

    return backupFs;
  }

  private static ExecutorService resolveDownloadExecutor(
      Optional<ExecutorService> customDownloadExecutorService,
      BaseHadoopShuffleClientConfiguration baseConfig) {
    return customDownloadExecutorService.orElseGet(
        () -> NamedExecutors.newFixedThreadExecutorService(
            baseConfig.downloadParallelism(), "async-plugin-hadoop-download-%d"));
  }

  private static ExecutorService resolveUploadExecutor(
      Optional<ExecutorService> customUploadExecutorService,
      BaseHadoopShuffleClientConfiguration baseConfig) {
    return customUploadExecutorService.orElseGet(
        () -> NamedExecutors.newFixedThreadExecutorService(
            baseConfig.uploadParallelism(), "async-plugin-hadoop-upload-%d"));
  }

  public static ShuffleClientBuilder builder() {
    return new ShuffleClientBuilder();
  }

  private ShuffleClients() {
  }
}
