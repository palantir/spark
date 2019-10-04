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

package org.apache.spark.shuffle.api;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import org.apache.spark.storage.BlockManagerId;

public interface ShuffleDriverComponents {

  /**
   * @return additional SparkConf values necessary for the executors.
   */
  Map<String, String> initializeApplication();

  default void cleanupApplication() throws IOException {}

  default void registerShuffle(int shuffleId) throws IOException {}

  default void removeShuffle(int shuffleId, boolean blocking) throws IOException {}

  default boolean isMapOutputAvailableWhenLocationLost(
      int shuffleId, int mapId, Optional<BlockManagerId> location) {
    return false;
  }

  default boolean unregisterOutputOnHostOnFetchFailure() {
    return true;
  }
}
