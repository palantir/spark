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

import org.apache.spark.annotation.Experimental;

import java.util.Iterator;

/**
 * :: Experimental ::
 * An interface for iterating through shuffle blocks to read.
 * @since 3.0.0
 */
@Experimental
public interface ShuffleReaderIterable extends Iterable<ShuffleReaderInputStream> {

  interface ShuffleReaderIterator extends Iterator<ShuffleReaderInputStream> {
    /**
     * Instructs the shuffle iterator to fetch the last block again. This is useful
     * if the block is determined to be corrupt after decryption or decompression.
     *
     * @throws Exception if current block cannot be retried.
     */
    default void retryLastBlock(Throwable t) throws Exception {
      throw new UnsupportedOperationException("Cannot retry fetching bad blocks", t);
    }
  }

  @Override
  ShuffleReaderIterator iterator();
}
