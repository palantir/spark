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

package org.apache.spark.shuffle

import java.io.{InputStream, IOException}
import java.nio.ByteBuffer

import scala.collection.JavaConverters._

import org.apache.spark._
import org.apache.spark.api.shuffle.{ShuffleBlockInfo, ShuffleReadSupport}
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.serializer.SerializerManager
import org.apache.spark.storage.ShuffleBlockId
import org.apache.spark.util.{CompletionIterator, Utils}
import org.apache.spark.util.collection.ExternalSorter
import org.apache.spark.util.io.ChunkedByteBufferOutputStream

/**
 * Fetches and reads the partitions in range [startPartition, endPartition) from a shuffle by
 * requesting them from other nodes' block stores.
 */
private[spark] class BlockStoreShuffleReader[K, C](
    handle: BaseShuffleHandle[K, _, C],
    startPartition: Int,
    endPartition: Int,
    context: TaskContext,
    readMetrics: ShuffleReadMetricsReporter,
    serializerManager: SerializerManager,
    shuffleReadSupport: ShuffleReadSupport,
    mapOutputTracker: MapOutputTracker = SparkEnv.get.mapOutputTracker,
    sparkConf: SparkConf = SparkEnv.get.conf)
  extends ShuffleReader[K, C] with Logging {

  private val dep = handle.dependency

  private val detectCorrupt = sparkConf.get(config.SHUFFLE_DETECT_CORRUPT)

  private val maxBytesInFlight = sparkConf.get(config.REDUCER_MAX_SIZE_IN_FLIGHT) * 1024 * 1024

  /** Read the combined key-values for this reduce task */
  override def read(): Iterator[Product2[K, C]] = {
    val streamsIterator =
      shuffleReadSupport.getPartitionReaders(new Iterable[ShuffleBlockInfo] {
        override def iterator: Iterator[ShuffleBlockInfo] = {
          mapOutputTracker.getMapSizesByExecutorId(handle.shuffleId, startPartition, endPartition)
            .flatMap { blockManagerIdInfo =>
              blockManagerIdInfo._2.map { blockInfo =>
                val block = blockInfo._1.asInstanceOf[ShuffleBlockId]
                new ShuffleBlockInfo(block.shuffleId, block.mapId, block.reduceId, blockInfo._2)
              }
            }
        }
      }.asJava).iterator()

    val retryingWrappedStreams = new Iterator[InputStream] {
      override def hasNext: Boolean = streamsIterator.hasNext

      override def next(): InputStream = {
        var returnStream: InputStream = null
        while (streamsIterator.hasNext && returnStream == null) {
          val nextStream = streamsIterator.next()
          val blockInfo = nextStream.getShuffleBlockInfo
          val blockId = ShuffleBlockId(
            blockInfo.getShuffleId,
            blockInfo.getMapId,
            blockInfo.getReduceId)
          if (detectCorrupt && blockInfo.getLength < maxBytesInFlight) {
            try {
              val in = serializerManager.wrapStream(blockId, nextStream.getInputStream)
              val out = new ChunkedByteBufferOutputStream(64 * 1024, ByteBuffer.allocate)
              // Decompress the whole block at once to detect any corruption, which could increase
              // the memory usage and potentially increase the chance of OOM.
              // TODO: manage the memory used here, and spill it into disk in case of OOM.
              Utils.copyStream(in, out, closeStreams = true)
              returnStream = out.toChunkedByteBuffer.toInputStream(dispose = true)
            } catch {
              case e: IOException =>
                streamsIterator.retryLastBlock(e)
            }
          } else {
            returnStream = serializerManager.wrapStream(blockId, nextStream.getInputStream)
          }
        }
        if (returnStream == null) {
          throw new IllegalStateException("Expected shuffle reader iterator to return a stream")
        }
        returnStream
      }
    }

    val serializerInstance = dep.serializer.newInstance()
    val recordIter = retryingWrappedStreams.flatMap { wrappedStream =>
      // Note: the asKeyValueIterator below wraps a key/value iterator inside of a
      // NextIterator. The NextIterator makes sure that close() is called on the
      // underlying InputStream when all records have been read.
      serializerInstance.deserializeStream(wrappedStream).asKeyValueIterator
    }

    // Update the context task metrics for each record read.
    val metricIter = CompletionIterator[(Any, Any), Iterator[(Any, Any)]](
      recordIter.map { record =>
        readMetrics.incRecordsRead(1)
        record
      },
      context.taskMetrics().mergeShuffleReadMetrics())

    // An interruptible iterator must be used here in order to support task cancellation
    val interruptibleIter = new InterruptibleIterator[(Any, Any)](context, metricIter)

    val aggregatedIter: Iterator[Product2[K, C]] = if (dep.aggregator.isDefined) {
      if (dep.mapSideCombine) {
        // We are reading values that are already combined
        val combinedKeyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, C)]]
        dep.aggregator.get.combineCombinersByKey(combinedKeyValuesIterator, context)
      } else {
        // We don't know the value type, but also don't care -- the dependency *should*
        // have made sure its compatible w/ this aggregator, which will convert the value
        // type to the combined type C
        val keyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, Nothing)]]
        dep.aggregator.get.combineValuesByKey(keyValuesIterator, context)
      }
    } else {
      interruptibleIter.asInstanceOf[Iterator[Product2[K, C]]]
    }

    // Sort the output if there is a sort ordering defined.
    val resultIter = dep.keyOrdering match {
      case Some(keyOrd: Ordering[K]) =>
        // Create an ExternalSorter to sort the data.
        val sorter =
          new ExternalSorter[K, C, C](context, ordering = Some(keyOrd), serializer = dep.serializer)
        sorter.insertAll(aggregatedIter)
        context.taskMetrics().incMemoryBytesSpilled(sorter.memoryBytesSpilled)
        context.taskMetrics().incDiskBytesSpilled(sorter.diskBytesSpilled)
        context.taskMetrics().incPeakExecutionMemory(sorter.peakMemoryUsedBytes)
        // Use completion callback to stop sorter if task was finished/cancelled.
        context.addTaskCompletionListener[Unit](_ => {
          sorter.stop()
        })
        CompletionIterator[Product2[K, C], Iterator[Product2[K, C]]](sorter.iterator, sorter.stop())
      case None =>
        aggregatedIter
    }

    resultIter match {
      case _: InterruptibleIterator[Product2[K, C]] => resultIter
      case _ =>
        // Use another interruptible iterator here to support task cancellation as aggregator
        // or(and) sorter may have consumed previous interruptible iterator.
        new InterruptibleIterator[Product2[K, C]](context, resultIter)
    }
  }
}
