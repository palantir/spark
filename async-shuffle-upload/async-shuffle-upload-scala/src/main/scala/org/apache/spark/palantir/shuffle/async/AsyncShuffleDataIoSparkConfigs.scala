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

package org.apache.spark.palantir.shuffle.async

import java.util.concurrent.TimeUnit

import scala.util.Try

import org.apache.spark.internal.config.ConfigBuilder
import org.apache.spark.network.util.ByteUnit
import org.apache.spark.palantir.shuffle.async.api.SparkShuffleApiConstants

object AsyncShuffleDataIoSparkConfigs {
  val BASE_URI = ConfigBuilder(SparkShuffleApiConstants.SHUFFLE_BASE_URI_CONF)
    .doc("Base URI for the backup files. Should be able to be loaded by a Hadoop file system.")
    .stringConf
    .createOptional

  val APP_NAME_DEPRECATED =
    ConfigBuilder(SparkShuffleApiConstants.SHUFFLE_PLUGIN_APP_NAME_CONF_DEPRECATED)
      .doc("Deprecated configuration value for the application name.")
      .stringConf
      .createOptional

  val APP_NAME = ConfigBuilder(SparkShuffleApiConstants.SHUFFLE_PLUGIN_APP_NAME_CONF)
    .doc("Application name to use for tagging shuffle files.")
    .fallbackConf(APP_NAME_DEPRECATED)

  val ENCRYPTION_ENABLED = ConfigBuilder(
    SparkShuffleApiConstants.SHUFFLE_PLUGIN_ENCRYPTION_ENABLED)
    .doc(
      "Controls whether to encrypt shuffle files or not before uploading them to remote storage")
    .booleanConf
    .createWithDefault(false)

  val ENCRYPTION_KEY_ALGORITHM =
    ConfigBuilder(SparkShuffleApiConstants.SHUFFLE_PLUGIN_ENCRYPTION_KEY_ALGORITHM)
      .doc("Algorithm used to generated the key pair used to encrypt the symmetric " +
        "key used to encrypt the shuffle files")
      .stringConf
      .createWithDefault("RSA")

  val ENCRYPTION_KEY_SIZE =
    ConfigBuilder(SparkShuffleApiConstants.SHUFFLE_PLUGIN_ENCRYPTION_KEY_SIZE)
      .doc("Key size used to generate the key pair used to encrypt the symmetric key")
      .intConf
      .createWithDefault(2048)

  val S3A_CREDS_FILE_DEPRECATED =
    ConfigBuilder(SparkShuffleApiConstants.SHUFFLE_S3A_CREDS_FILE_CONF_DEPRECATED)
      .doc("Deprecated version of configuration pointing to s3 credentials.")
      .stringConf
      .createOptional

  val S3A_CREDS_FILE = ConfigBuilder(SparkShuffleApiConstants.SHUFFLE_S3A_CREDS_FILE_CONF)
    .doc("File pointing to the Amazon AWS credentials when backing up shuffles to S3.")
    .fallbackConf(S3A_CREDS_FILE_DEPRECATED)

  val S3A_ENDPOINT = ConfigBuilder(SparkShuffleApiConstants.SHUFFLE_S3A_ENDPOINT_CONF)
    .doc("If the shuffle client is configured for s3a, this specifies the s3a endpoint")
    .stringConf
    .createOptional

  val DEFAULT_DOWNLOAD_PARALLELISM = 10

  val DOWNLOAD_PARALLELISM = ConfigBuilder("spark.shuffle.hadoop.async.download.parallelism")
    .doc("Number of threads in the thread pool that download backup shuffle files.")
    .intConf
    .createWithDefault(DEFAULT_DOWNLOAD_PARALLELISM)

  val DEFAULT_UPLOAD_PARALLELISM = 5

  val UPLOAD_PARALLELISM = ConfigBuilder("spark.shuffle.hadoop.async.upload.parallelism")
    .doc("Number of threads in the thread pool that uploads backup shuffle files.")
    .intConf
    .createWithDefault(DEFAULT_UPLOAD_PARALLELISM)

  val STORAGE_STRATEGY = ConfigBuilder("spark.shuffle.hadoop.async.storage.strategy")
    .doc("Strategy to use for uploading shuffle files to the backing store.")
    .stringConf
    .checkValues(Set("basic", "merging"))
    .createWithDefault("basic")

  val DEFAULT_READ_LOCAL_DISK_PARALLELISM = 5

  val READ_LOCAL_DISK_PARALLELISM =
    ConfigBuilder("spark.shuffle.hadoop.async.storage.merging.local.read.parallelism")
      .doc(
        "When using the \"merging\" storage strategy, files are downloaded to local disk" +
          " before map output data is shipped back to the shuffle reader. This configuration" +
          " controls the parallelism for the local disk reads after files are downloaded.")
      .intConf
      .createWithDefault(DEFAULT_READ_LOCAL_DISK_PARALLELISM)

  // Default merged batch size of 10MB
  val DEFAULT_MERGING_STORAGE_BATCH_SIZE_BYTES = 10 * 1024 * 1024

  val MERGED_BATCH_SIZE = ConfigBuilder("spark.shuffle.hadoop.async.storage.merging.batch.size")
    .doc("Approximate target size for the combined shuffle files when using the merging storage" +
      " strategy. The merging storage strategy combines shuffle files before uploading them to" +
      " the backing store - so this configuration sets a target size for the combined files" +
      " before they are uploaded. Note that large shuffle files may cause the uploaded blob" +
      " to exceed this threshold.")
    .bytesConf(ByteUnit.BYTE)
    .createWithDefault(DEFAULT_MERGING_STORAGE_BATCH_SIZE_BYTES)

  val DEFAULT_MERGING_MAX_BUFFERED_INPUTS = 2048

  val MERGING_MAX_BUFFERED_INPUTS =
    ConfigBuilder("spark.shuffle.hadoop.async.storage.merging.buffer.max")
      .doc("Number of inputs that can be buffered on the executor before the shuffle uploader" +
        " forces all files to be uploaded to the backing store. This is to prevent the merging" +
        " buffer from growing without bound.")
      .intConf
      .createWithDefault(DEFAULT_MERGING_MAX_BUFFERED_INPUTS)

  // 5 second default uploading polling period
  val DEFAULT_UPLOAD_POLLING_PERIOD_MILLIS = 5 * 1000

  val UPLOAD_POLLING_PERIOD =
    ConfigBuilder("spark.shuffle.hadoop.async.storage.merging.upload.period")
      .doc("Interval at which the executors will check for uploading shuffle data.")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefault(DEFAULT_UPLOAD_POLLING_PERIOD_MILLIS)

  val DEFAULT_MERGED_BATCH_MAXIMUM_BUFFERED_AGE_MINUTES = 5

  val MERGED_BATCH_MAXIMUM_BUFFERED_AGE =
    ConfigBuilder("spark.shuffle.hadoop.async.storage.merging.buffer.age")
      .doc("Amount of time the executor will wait before uploading all the files for a batch.")
      .timeConf(TimeUnit.MINUTES)
      .createWithDefault(DEFAULT_MERGED_BATCH_MAXIMUM_BUFFERED_AGE_MINUTES)

  val DEFAULT_STREAM_BUFFER_SIZE_BYTES = 128 * 1024

  val STREAM_BUFFER_SIZE =
    ConfigBuilder("spark.shuffle.hadoop.async.io.buffer.size")
      .doc(
        "Size of buffers when reading and writing data from storage, both remote and on local" +
          " disk.")
      .bytesConf(ByteUnit.BYTE)
      .checkValue(validator = bytes => {
        Try {
          Math.toIntExact(bytes)
          true
        } getOrElse false
      }, errorMsg = s"Buffer size must be less than ${Integer.MAX_VALUE}")
      .createWithDefault(DEFAULT_STREAM_BUFFER_SIZE_BYTES)

  val LOCAL_FILE_STREAM_BUFFER_SIZE =
    ConfigBuilder("spark.shuffle.hadoop.async.io.disk.buffer.size")
      .doc("Size of buffers when reading and writing data on local disk")
      .fallbackConf(STREAM_BUFFER_SIZE)

  val DOWNLOAD_SHUFFLE_BLOCK_BUFFER_SIZE =
    ConfigBuilder("spark.shuffle.hadoop.async.io.download.buffer.size")
      .doc(
        "Size of buffers to use when downloading shuffle blocks from remote storage down to" +
          " local disk.")
      .fallbackConf(STREAM_BUFFER_SIZE)

  val DOWNLOAD_SHUFFLE_BLOCKS_IN_MEMORY_MAX_SIZE =
    ConfigBuilder("spark.shuffle.hadoop.async.io.download.block.memory.max")
      .doc("The maximum amount of memory in bytes that a block can have for it to be downloaded" +
        " to memory. If a shuffle block were to be downloaded that is larger than this value, it" +
        " will be downloaded to disk instead.")
      .fallbackConf(org.apache.spark.internal.config.MAX_REMOTE_BLOCK_SIZE_FETCH_TO_MEM)

  val DEFAULT_DRIVER_REF_CACHE_MAX_SIZE = 1024

  val DRIVER_REF_CACHE_MAX_SIZE =
    ConfigBuilder("spark.shuffle.hadoop.async.driverref.cache.size")
      .doc(
        "Maximum size of the cache used by each executor to avoid calling the driver in" +
          " excess amounts.")
      .intConf
      .createWithDefault(DEFAULT_DRIVER_REF_CACHE_MAX_SIZE)

  // Default of 1 minute for the driver ref cache's expiration period.
  val DEFAULT_DRIVER_REF_CACHE_EXPIRATION_PERIOD_MILLIS = 60000

  val DRIVER_REF_CACHE_EXPIRATION_PERIOD =
    ConfigBuilder("spark.shuffle.hadoop.driverref.cache.expiration.period")
      .doc("Duration results from communicating with the driver will be cached on the executor" +
        " before being invalidated.")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefault(DEFAULT_DRIVER_REF_CACHE_EXPIRATION_PERIOD_MILLIS)

  val METRICS_FACTORY_CLASS_DEPRECATED = ConfigBuilder(
    SparkShuffleApiConstants.METRICS_FACTORY_CLASS_CONF_DEPRECATED)
    .doc("Deprecated config value for the metrics factory class.")
    .stringConf
    .createWithDefault(
      "org.apache.spark.palantir.shuffle.async.metrics.slf4j.Slf4JHadoopAsyncShuffleMetricsFactory")

  val METRICS_FACTORY_CLASS = ConfigBuilder(SparkShuffleApiConstants.METRICS_FACTORY_CLASS_CONF)
    .doc("Implementation of the metrics system when using the plugin.")
    .fallbackConf(METRICS_FACTORY_CLASS_DEPRECATED)

  val CACHE_INDEX_FILES_LOCALLY =
    ConfigBuilder("spark.shuffle.hadoop.async.io.basic.cache.index.enable")
      .doc("Cache the index files locally on the executor disk")
      .booleanConf
      .createWithDefault(false)

  val PREFER_DOWNLOAD_FROM_HADOOP_DEPRECATED =
    ConfigBuilder("spark.shuffle.hadoop.async.io.download.prefer.s3")
      .doc("Deprecated variant of preferring downloading blocks from remote storage.")
      .booleanConf
      .createWithDefault(false)

  val PREFER_DOWNLOAD_FROM_HADOOP =
    ConfigBuilder("spark.shuffle.hadoop.async.io.download.prefer.hadoop")
      .doc(
        "Prefer download from remote storage over download from another executor if the" +
          " shuffle file exists on both.")
      .fallbackConf(PREFER_DOWNLOAD_FROM_HADOOP_DEPRECATED)

  val S3A_UPLOAD_MULTIPART_TYPE =
    ConfigBuilder("spark.shuffle.hadoop.async.io.upload.multipart.buffer.type")
      .doc("The type of multipart to use. See hadoop config `fs.s3a.fast.upload.buffer`")
      .stringConf
      .createWithDefault("disk")

  val S3A_UPLOAD_MULTIPART_SIZE =
    ConfigBuilder("spark.shuffle.hadoop.async.io.upload.multipart.size")
      .doc("The size of each multipart upload. See hadoop config `fs.s3a.multipart.size`")
      .stringConf
      .createWithDefault("100M")
}
