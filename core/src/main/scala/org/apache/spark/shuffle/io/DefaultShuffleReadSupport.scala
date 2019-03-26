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

package org.apache.spark.shuffle.io

import java.io.InputStream
import java.lang

import scala.collection.JavaConverters._

import org.apache.spark.{MapOutputTracker, SparkEnv, TaskContext}
import org.apache.spark.api.shuffle.{ShuffleBlockInfo, ShuffleReadSupport}
import org.apache.spark.internal.config
import org.apache.spark.serializer.SerializerManager
import org.apache.spark.storage.{BlockId, BlockManager, ShuffleBlockFetcherIterator, ShuffleBlockId}

class DefaultShuffleReadSupport(
    blockManager: BlockManager,
    serializerManager: SerializerManager,
    mapOutputTracker: MapOutputTracker) extends ShuffleReadSupport {

  val maxBytesInFlight = SparkEnv.get.conf.get(config.REDUCER_MAX_SIZE_IN_FLIGHT) * 1024 * 1024
  val maxReqsInFlight = SparkEnv.get.conf.get(config.REDUCER_MAX_REQS_IN_FLIGHT)
  val maxBlocksInFlightPerAddress =
    SparkEnv.get.conf.get(config.REDUCER_MAX_BLOCKS_IN_FLIGHT_PER_ADDRESS)
  val maxReqSizeShuffleToMem = SparkEnv.get.conf.get(config.MAX_REMOTE_BLOCK_SIZE_FETCH_TO_MEM)
  val detectCorrupt = SparkEnv.get.conf.get(config.SHUFFLE_DETECT_CORRUPT)

  override def getPartitionReaders(
      blockMetadata: lang.Iterable[ShuffleBlockInfo]): lang.Iterable[InputStream] = {

    val minReduceId = blockMetadata.asScala.map(block => block.getReduceId).min
    val maxReduceId = blockMetadata.asScala.map(block => block.getReduceId).max
    val shuffleId = blockMetadata.asScala.head.getShuffleId

    val shuffleBlockFetchIterator = new ShuffleBlockFetcherIterator(
      TaskContext.get(),
      blockManager.shuffleClient,
      blockManager,
      mapOutputTracker.getMapSizesByExecutorId(shuffleId, minReduceId, maxReduceId + 1),
      serializerManager.wrapStream,
      maxBytesInFlight,
      maxReqsInFlight,
      maxBlocksInFlightPerAddress,
      maxReqSizeShuffleToMem,
      detectCorrupt,
      shuffleMetrics = TaskContext.get().taskMetrics().createTempShuffleReadMetrics()
    ).toCompletionIterator

    new ShuffleBlockInputStreamIterator(shuffleBlockFetchIterator).toIterable.asJava
  }

  private class ShuffleBlockInputStreamIterator(
      blockFetchIterator: Iterator[(BlockId, InputStream)])
    extends Iterator[InputStream] {
    override def hasNext: Boolean = blockFetchIterator.hasNext

    override def next(): InputStream = {
      blockFetchIterator.next()._2
    }
  }

  private[spark] object DefaultShuffleReadSupport {
    def toShuffleBlockInfo(blockId: BlockId, length: Long): ShuffleBlockInfo = {
      assert(blockId.isInstanceOf[ShuffleBlockId])
      val shuffleBlockId = blockId.asInstanceOf[ShuffleBlockId]
      new ShuffleBlockInfo(
        shuffleBlockId.shuffleId,
        shuffleBlockId.mapId,
        shuffleBlockId.reduceId,
        length)
    }
  }
}
