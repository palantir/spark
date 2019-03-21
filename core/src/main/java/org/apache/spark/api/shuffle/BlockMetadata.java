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

package org.apache.spark.api.shuffle;

import org.apache.spark.storage.BlockManagerId;

import java.util.Optional;

public final class BlockMetadata {
  private final int shuffleId;
  private final int mapId;
  private final int reduceId;
  private final long length;
  private final Optional<BlockManagerId> shuffleLocation;

  private BlockMetadata(
      int shuffleId,
      int mapId,
      int reduceId,
      long length,
      Optional<BlockManagerId> shuffleLocation) {
    this.shuffleId = shuffleId;
    this.mapId = mapId;
    this.reduceId = reduceId;
    this.length = length;
    this.shuffleLocation = shuffleLocation;
  }

  public static BlockMetadata create(int shuffleId, int mapId, int reduceId, long length) {
    return new BlockMetadata(shuffleId, mapId, reduceId, length, Optional.empty());
  }

  public static BlockMetadata create(
      int shuffleId,
      int mapId,
      int reduceId,
      long length,
      BlockManagerId blockManagerId) {
    return new BlockMetadata(shuffleId, mapId, reduceId, length, Optional.of(blockManagerId));
  }

  public int getShuffleId() {
    return shuffleId;
  }

  public int getMapId() {
    return mapId;
  }

  public int getReduceId() {
    return reduceId;
  }

  public long getLength() {
    return length;
  }

  public Optional<BlockManagerId> getShuffleLocation() {
    return shuffleLocation;
  }
}
