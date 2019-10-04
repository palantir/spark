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

package org.apache.spark.shuffle.sort.lifecycle;

import java.util.Map;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.shuffle.api.ShuffleDriverComponents;
import org.apache.spark.storage.BlockManagerMaster;

public class LocalDiskShuffleDriverComponents implements ShuffleDriverComponents {

  private BlockManagerMaster blockManagerMaster;
  private final SparkConf sparkConf;

  public LocalDiskShuffleDriverComponents(SparkConf sparkConf) {
    this.sparkConf = sparkConf;
  }

  @VisibleForTesting
  public LocalDiskShuffleDriverComponents(BlockManagerMaster blockManagerMaster) {
    this.sparkConf = new SparkConf(false);
    this.blockManagerMaster = blockManagerMaster;
  }

  @VisibleForTesting
  public LocalDiskShuffleDriverComponents(
      SparkConf sparkConf, BlockManagerMaster blockManagerMaster) {
    this.sparkConf = sparkConf;
    this.blockManagerMaster = blockManagerMaster;
  }

  @Override
  public Map<String, String> initializeApplication() {
    blockManagerMaster = SparkEnv.get().blockManager().master();
    return ImmutableMap.of();
  }

  @Override
  public void removeShuffle(int shuffleId, boolean blocking) {
    checkInitialized();
    blockManagerMaster.removeShuffle(shuffleId, blocking);
  }

  @Override
  public boolean unregisterOutputOnHostOnFetchFailure() {
    boolean unregisterOutputOnHostOnFetchFailure = Boolean.parseBoolean(
        sparkConf.get(
            org.apache.spark.internal.config.package$.MODULE$
                .UNREGISTER_OUTPUT_ON_HOST_ON_FETCH_FAILURE().key(),
            org.apache.spark.internal.config.package$.MODULE$
                .UNREGISTER_OUTPUT_ON_HOST_ON_FETCH_FAILURE().defaultValueString()));
    boolean externalShuffleServiceEnabled = Boolean.parseBoolean(
        sparkConf.get(
            org.apache.spark.internal.config.package$.MODULE$
                .SHUFFLE_SERVICE_ENABLED().key(),
            org.apache.spark.internal.config.package$.MODULE$
                .SHUFFLE_SERVICE_ENABLED().defaultValueString()));
    return unregisterOutputOnHostOnFetchFailure && externalShuffleServiceEnabled;
  }

  private void checkInitialized() {
    if (blockManagerMaster == null) {
      throw new IllegalStateException("Driver components must be initialized before using");
    }
  }
}
