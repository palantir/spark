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

import java.util.Optional;

import org.apache.spark.annotation.Private;
import org.apache.spark.storage.BlockManagerId;

@Private
public final class MapOutputWriterCommitMessage {

  private final long[] partitionLengths;
  private final Optional<BlockManagerId> location;

  private MapOutputWriterCommitMessage(
      long[] partitionLengths, Optional<BlockManagerId> location) {
    this.partitionLengths = partitionLengths;
    this.location = location;
  }

  public static MapOutputWriterCommitMessage of(long[] partitionLengths) {
    return new MapOutputWriterCommitMessage(partitionLengths, Optional.empty());
  }

  public static MapOutputWriterCommitMessage of(
      long[] partitionLengths, BlockManagerId location) {
    return new MapOutputWriterCommitMessage(partitionLengths, Optional.of(location));
  }

  public long[] getPartitionLengths() {
    return partitionLengths;
  }

  public Optional<BlockManagerId> getLocation() {
    return location;
  }
}
