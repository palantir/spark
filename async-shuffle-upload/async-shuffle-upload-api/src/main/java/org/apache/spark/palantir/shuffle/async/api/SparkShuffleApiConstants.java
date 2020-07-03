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

package org.apache.spark.palantir.shuffle.async.api;

public final class SparkShuffleApiConstants {

  // Identifiers used by the spark shuffle plugin
  public static final String SHUFFLE_BASE_URI_CONF = "spark.shuffle.hadoop.async.base-uri";

  public static final String SHUFFLE_PLUGIN_APP_NAME_CONF = "spark.shuffle.hadoop.async.appName";
  // Deprecated configurations are picked up as fallbacks when their newer counterparts are not
  // specified.
  public static final String SHUFFLE_PLUGIN_APP_NAME_CONF_DEPRECATED =
      "spark.plugin.shuffle.async.appName";

  public static final String SHUFFLE_PLUGIN_ENCRYPTION_ENABLED =
      "spark.plugin.shuffle.async.encryption.enabled";
  public static final String SHUFFLE_PLUGIN_ENCRYPTION_KEY_ALGORITHM =
      "spark.plugin.shuffle.async.encryption.key.algorithm";
  public static final String SHUFFLE_PLUGIN_ENCRYPTION_KEY_SIZE =
      "spark.plugin.shuffle.async.encryption.key.size";

  public static final String SHUFFLE_S3A_CREDS_FILE_CONF =
      "spark.shuffle.hadoop.async.s3a.credsFile";
  public static final String SHUFFLE_S3A_CREDS_FILE_CONF_DEPRECATED =
      "spark.plugin.shuffle.async.s3a.credsFile";

  public static final String SHUFFLE_S3A_ENDPOINT_CONF =
      "spark.shuffle.hadoop.async.s3a.endpoint";

  public static final String METRICS_FACTORY_CLASS_CONF =
      "spark.shuffle.hadoop.async.metrics.factory.class";
  public static final String METRICS_FACTORY_CLASS_CONF_DEPRECATED =
      "spark.plugin.shuffle.async.metricsFactoryClass";

  private SparkShuffleApiConstants() {}

}
