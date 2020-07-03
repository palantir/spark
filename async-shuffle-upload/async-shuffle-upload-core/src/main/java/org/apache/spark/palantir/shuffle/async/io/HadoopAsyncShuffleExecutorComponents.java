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

import java.io.IOException;
import java.time.Clock;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

import scala.compat.java8.OptionConverters;

import com.google.common.annotations.VisibleForTesting;
import com.palantir.logsafe.SafeArg;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.TaskContext;
import org.apache.spark.internal.config.package$;
import org.apache.spark.io.CompressionCodec;
import org.apache.spark.io.CompressionCodec$;
import org.apache.spark.palantir.shuffle.async.AsyncShuffleDataIoSparkConfigs;
import org.apache.spark.palantir.shuffle.async.AsyncShuffleUploadDriverEndpoint;
import org.apache.spark.palantir.shuffle.async.JavaShuffleDriverEndpointRef;
import org.apache.spark.palantir.shuffle.async.ShuffleDriverEndpointRef;
import org.apache.spark.palantir.shuffle.async.api.SparkShuffleApiConstants;
import org.apache.spark.palantir.shuffle.async.client.BaseHadoopShuffleClientConfiguration;
import org.apache.spark.palantir.shuffle.async.client.ShuffleClient;
import org.apache.spark.palantir.shuffle.async.client.ShuffleClients;
import org.apache.spark.palantir.shuffle.async.metrics.HadoopAsyncShuffleMetrics;
import org.apache.spark.palantir.shuffle.async.metrics.HadoopAsyncShuffleMetricsFactory;
import org.apache.spark.palantir.shuffle.async.metrics.slf4j.Slf4JHadoopAsyncShuffleMetricsFactory;
import org.apache.spark.palantir.shuffle.async.util.Suppliers;
import org.apache.spark.serializer.SerializerManager;
import org.apache.spark.shuffle.IndexShuffleBlockResolver;
import org.apache.spark.shuffle.api.ShuffleBlockInfo;
import org.apache.spark.shuffle.api.ShuffleBlockInputStream;
import org.apache.spark.shuffle.api.ShuffleExecutorComponents;
import org.apache.spark.shuffle.api.ShuffleMapOutputWriter;
import org.apache.spark.util.RpcUtils;

public final class HadoopAsyncShuffleExecutorComponents implements ShuffleExecutorComponents {

  private static final Logger LOG = LoggerFactory.getLogger(
      HadoopAsyncShuffleExecutorComponents.class);

  private final ShuffleExecutorComponents delegate;
  private final SparkConf sparkConf;
  private final Optional<Clock> customClock;
  private final Optional<ExecutorService> customUploadExecutorService;
  private final Optional<ExecutorService> customDownloadExecutorService;
  private final Optional<ScheduledExecutorService> customUploadCoordinatorExecutorService;
  private final Supplier<SparkEnv> sparkEnvSupplier;
  private final Supplier<ShuffleFileLocator> shuffleFileLocatorSupplier;
  private final Supplier<CompressionCodec> compressionCodecSupplier;
  private final Supplier<HadoopAsyncShuffleMetricsFactory> metrics;

  private SerializerManager serializerManager;
  private Optional<ShuffleClient> maybeClient;
  private boolean shouldCompressShuffle;
  private ShuffleDriverEndpointRef shuffleDriverEndpointRef;

  // Read support is split off primarily to reduce the number of lines in the class.
  private Optional<HadoopAsyncShuffleReadSupport> maybeReadSupport;

  public HadoopAsyncShuffleExecutorComponents(
      SparkConf sparkConf,
      ShuffleExecutorComponents delegate) {
    this(
        sparkConf,
        delegate,
        Optional.empty(),
        SparkEnv::get,
        Suppliers.memoize(() -> new DefaultShuffleFileLocator(
            new IndexShuffleBlockResolver(sparkConf, SparkEnv.get().blockManager()))),
        Suppliers.memoize(() -> CompressionCodec$.MODULE$.createCodec(sparkConf)),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty());
  }

  @VisibleForTesting
  HadoopAsyncShuffleExecutorComponents(
      SparkConf sparkConf,
      ShuffleExecutorComponents delegate,
      Optional<Clock> customClock,
      Supplier<SparkEnv> sparkEnvSupplier,
      Supplier<ShuffleFileLocator> shuffleFileLocatorSupplier,
      Supplier<CompressionCodec> compressionCodecSupplier,
      Optional<ExecutorService> customUploadExecutorService,
      Optional<ExecutorService> customDownloadExecutorService,
      Optional<ScheduledExecutorService> customUploadCoordinatorExecutorService,
      Optional<Supplier<HadoopAsyncShuffleMetricsFactory>> metrics) {
    this.sparkConf = sparkConf;
    this.delegate = delegate;
    this.customClock = customClock;
    this.sparkEnvSupplier = sparkEnvSupplier;
    this.shuffleFileLocatorSupplier = shuffleFileLocatorSupplier;
    this.compressionCodecSupplier = compressionCodecSupplier;
    this.customUploadExecutorService = customUploadExecutorService;
    this.customDownloadExecutorService = customDownloadExecutorService;
    this.customUploadCoordinatorExecutorService = customUploadCoordinatorExecutorService;
    this.metrics = metrics.orElseGet(() -> this::loadMetricsFactory);
  }

  @Override
  public void initializeExecutor(
      String execAppId, String execId, Map<String, String> extraConfigs) {
    delegate.initializeExecutor(execAppId, execId, extraConfigs);
    SparkEnv sparkEnv = sparkEnvSupplier.get();
    BaseHadoopShuffleClientConfiguration shuffleClientConf =
        BaseHadoopShuffleClientConfiguration.of(sparkConf);
    Clock resolvedClock = customClock.orElseGet(Clock::systemUTC);
    shuffleDriverEndpointRef = createDriverEndpointRef(
        sparkEnv, shuffleClientConf);
    String appName = getShufflePluginAppName(sparkConf).orElse(execAppId);
    HadoopAsyncShuffleMetrics resolvedMetrics = metrics.get().create(sparkConf, appName);
    this.serializerManager = sparkEnv.serializerManager();
    this.shouldCompressShuffle = (boolean) sparkConf.get(package$.MODULE$.SHUFFLE_COMPRESS());
    this.maybeClient = ShuffleClients.builder()
        .appId(execAppId)
        .clock(resolvedClock)
        .driverEndpointRef(shuffleDriverEndpointRef)
        .customDownloadExecutorService(customDownloadExecutorService)
        .customUploadExecutorService(customUploadExecutorService)
        .customUploadCoordinatorExecutorService(customUploadCoordinatorExecutorService)
        .sparkConf(sparkConf)
        .metrics(resolvedMetrics)
        .extraConfigs(extraConfigs)
        .build();
    resolvedMetrics.markUsingAsyncShuffleUploadPlugin();
    this.maybeReadSupport = maybeClient.map(client ->
        new HadoopAsyncShuffleReadSupport(
            delegate,
            client,
            serializerManager,
            compressionCodecSupplier.get(),
            shouldCompressShuffle,
            resolvedMetrics.hadoopFetcherIteratorMetrics(),
            () -> Optional.ofNullable(TaskContext.get()),
            shuffleDriverEndpointRef,
            shuffleClientConf.preferDownloadFromHadoop()));
  }

  @Override
  public ShuffleMapOutputWriter createMapOutputWriter(
      int shuffleId, int mapId, long mapTaskAttemptId, int numPartitions) throws IOException {
    LOG.debug("Created MapOutputWriter for shuffle partition with numPartitions",
        SafeArg.of("shuffleId", shuffleId),
        SafeArg.of("mapId", mapId),
        SafeArg.of("attemptId", mapTaskAttemptId),
        SafeArg.of("numPartitions", numPartitions));
    ShuffleMapOutputWriter delegateWriter = delegate.createMapOutputWriter(
        shuffleId, mapId, mapTaskAttemptId, numPartitions);
    return maybeClient.<ShuffleMapOutputWriter>map(client ->
        new HadoopAsyncShuffleMapOutputWriter(
            delegateWriter,
            client,
            shuffleFileLocatorSupplier.get(),
            shuffleDriverEndpointRef,
            shuffleId,
            mapId,
            mapTaskAttemptId
        ))
        .orElse(delegateWriter);
  }

  @Override
  public Iterable<ShuffleBlockInputStream> getPartitionReaders(
      Iterable<ShuffleBlockInfo> blockMetadata) throws IOException {
    if (maybeReadSupport.isPresent()) {
      return maybeReadSupport.get().getPartitionReaders(blockMetadata);
    } else {
      return delegate.getPartitionReaders(blockMetadata);
    }
  }

  @Override
  public boolean shouldWrapPartitionReaderStream() {
    // Both of these return false, but still check for presence to match pattern-wise and to
    // future-proof against changes in the underlying local disk impl
    if (maybeClient.isPresent()) {
      return false;
    } else {
      return delegate.shouldWrapPartitionReaderStream();
    }
  }

  private static ShuffleDriverEndpointRef createDriverEndpointRef(
      SparkEnv sparkEnv, BaseHadoopShuffleClientConfiguration baseConfig) {
    return new JavaShuffleDriverEndpointRef(
        RpcUtils.makeDriverRef(
            AsyncShuffleUploadDriverEndpoint.NAME(),
            baseConfig.sparkConf(),
            sparkEnv.rpcEnv()),
        sparkEnv.blockManager().blockManagerId());
  }

  private HadoopAsyncShuffleMetricsFactory loadMetricsFactory() {
    String factoryClassName = sparkConf.get(AsyncShuffleDataIoSparkConfigs.METRICS_FACTORY_CLASS());
    try {
      return Class.forName(factoryClassName)
          .asSubclass(HadoopAsyncShuffleMetricsFactory.class)
          .getDeclaredConstructor()
          .newInstance();
    } catch (Exception e) {
      LOG.error("Failed to load async shuffle plugin's metrics factory specified by the Spark" +
              "configuration. Falling back to SLF4J-logging based implementation.",
          SafeArg.of("confKey", SparkShuffleApiConstants.METRICS_FACTORY_CLASS_CONF),
          SafeArg.of("metricsFactoryClass", factoryClassName),
          e);
      return new Slf4JHadoopAsyncShuffleMetricsFactory();
    }
  }

  private static Optional<String> getShufflePluginAppName(SparkConf sparkConf) {
    return OptionConverters.toJava(sparkConf.get(AsyncShuffleDataIoSparkConfigs.APP_NAME()));
  }
}
