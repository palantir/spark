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

import org.apache.spark.storage.BlockId;

import java.io.InputStream;

/**
 * :: Experimental ::
 * An object defining the shuffle block and length metadata associated with the block.
 * @since 3.0.0
 */
public class ShuffleBlockInputStream {
  private final BlockId blockId;
  private final InputStream inputStream;

  public ShuffleBlockInputStream(BlockId blockId, InputStream inputStream) {
    this.blockId = blockId;
    this.inputStream = inputStream;
  }

  public BlockId getBlockId() {
    return this.blockId;
  }

  public InputStream getInputStream() {
    return this.inputStream;
  }
}
