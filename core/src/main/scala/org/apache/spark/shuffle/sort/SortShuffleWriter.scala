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

package org.apache.spark.shuffle.sort

import org.apache.spark._
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.{ShuffleAggregationManager, IndexShuffleBlockResolver, ShuffleWriter, BaseShuffleHandle}
import org.apache.spark.storage.ShuffleBlockId
import org.apache.spark.util.collection.ExternalSorter

import scala.reflect.ClassTag

private[spark] class SortShuffleWriter[K, V, C](
    shuffleBlockResolver: IndexShuffleBlockResolver,
    handle: BaseShuffleHandle[K, V, C],
    mapId: Int,
    context: TaskContext)
  extends ShuffleWriter[K, V] with Logging {

  private val dep = handle.dependency

  private val blockManager = SparkEnv.get.blockManager

  private var sorter: SortShuffleFileWriter[K, _] = null

  // Are we in the process of stopping? Because map tasks can call stop() with success = true
  // and then call stop() with success = false if they get an exception, we want to make sure
  // we don't try deleting files, etc twice.
  private var stopping = false

  private var mapStatus: MapStatus = null

  private val writeMetrics = new ShuffleWriteMetrics()
  context.taskMetrics.shuffleWriteMetrics = Some(writeMetrics)

  /** Write a bunch of records to this task's output */
  override def write(records: Iterator[Product2[K, V]]): Unit = {
    // Decide if it's optimal to do the pre-aggregation.
    val aggManager = new ShuffleAggregationManager[K, V](SparkEnv.get.conf, records)
    // Short-circuit if dep.mapSideCombine is explicitly set to false so we don't do the
    // aggregation check. Else check if pre-aggregation would actually be beneficial
    // via aggManager.
    val enableMapSideCombine = dep.mapSideCombine && aggManager.enableAggregation()

    sorter = (dep.mapSideCombine, enableMapSideCombine) match {
      case (_, true) =>
        assert (dep.mapSideCombine)
        require(dep.aggregator.isDefined, "Map-side combine requested without " +
                                          "Aggregator specified!")
        val selectedSorter = new ExternalSorter[K, V, C](
          dep.aggregator, Some(dep.partitioner), dep.keyOrdering, dep.serializer)
        writeInternal(aggManager.getRestoredIterator(), selectedSorter)
        selectedSorter
      case (true, false) =>
        // The user requested map-side combine, but we determined that it would be sub-optimal
        // for here. So just write out the initial combiners (as the reducer will expect values
        //  of type "C") but don't do the aggregations itself
        require (dep.aggregator.isDefined, "Map side combine requested with " +
                                            "no aggregator specified!")
        val selectedSorter = if (SortShuffleWriter.shouldBypassMergeSort(SparkEnv.get.conf,
            dep.partitioner.numPartitions, aggregator = None, keyOrdering = None)) {
          new BypassMergeSortShuffleWriter[K, C](SparkEnv.get.conf, blockManager, dep.partitioner,
            writeMetrics, Serializer.getSerializer(dep.serializer))
        } else {
          new ExternalSorter[K, C, C](
            aggregator = None, Some(dep.partitioner), ordering = None, dep.serializer)
        }
        val definedAggregator = dep.aggregator.get
        val recordsWithAppliedCreateCombiner = aggManager.getRestoredIterator.map(kv =>
                                              (kv._1, definedAggregator.createCombiner(kv._2)))
        writeInternal(recordsWithAppliedCreateCombiner, selectedSorter)
        selectedSorter
      case (false, _) => {
        val selectedSorter = if (SortShuffleWriter.shouldBypassMergeSort(SparkEnv.get.conf,
            dep.partitioner.numPartitions, aggregator = None, keyOrdering = None)) {
          new BypassMergeSortShuffleWriter[K, V](SparkEnv.get.conf, blockManager, dep.partitioner,
            writeMetrics, Serializer.getSerializer(dep.serializer))
        } else {
          new ExternalSorter[K, V, V](
            aggregator = None, Some(dep.partitioner), ordering = None, dep.serializer)
        }
        writeInternal(aggManager.getRestoredIterator(), selectedSorter)
        selectedSorter
      }
    }
    logInfo("SortShuffleWriter - Enable Pre-Aggregation: " + enableMapSideCombine)
  }

  private def writeInternal[VorC](records: Iterator[Product2[K, VorC]],
      selectedSorter: SortShuffleFileWriter[K, VorC]): Unit = {
    selectedSorter.insertAll(records)
    // Don't bother including the time to open the merged output file in the shuffle write time,
    // because it just opens a single file, so is typically too fast to measure accurately
    // (see SPARK-3570).
    val outputFile = shuffleBlockResolver.getDataFile(dep.shuffleId, mapId)
    val blockId = ShuffleBlockId(dep.shuffleId, mapId, IndexShuffleBlockResolver.NOOP_REDUCE_ID)
    val partitionLengths = selectedSorter.writePartitionedFile(blockId, context, outputFile)
    shuffleBlockResolver.writeIndexFile(dep.shuffleId, mapId, partitionLengths)

    mapStatus = MapStatus(blockManager.shuffleServerId, partitionLengths)
  }

  /** Close this writer, passing along whether the map completed */
  override def stop(success: Boolean): Option[MapStatus] = {
    try {
      if (stopping) {
        return None
      }
      stopping = true
      if (success) {
        return Option(mapStatus)
      } else {
        // The map task failed, so delete our output data.
        shuffleBlockResolver.removeDataByMap(dep.shuffleId, mapId)
        return None
      }
    } finally {
      // Clean up our sorter, which may have its own intermediate files
      if (sorter != null) {
        val startTime = System.nanoTime()
        sorter.stop()
        context.taskMetrics.shuffleWriteMetrics.foreach(
          _.incShuffleWriteTime(System.nanoTime - startTime))
        sorter = null
      }
    }
  }
}

private[spark] object SortShuffleWriter {
  def shouldBypassMergeSort(
      conf: SparkConf,
      numPartitions: Int,
      aggregator: Option[Aggregator[_, _, _]],
      keyOrdering: Option[Ordering[_]]): Boolean = {
    val bypassMergeThreshold: Int = conf.getInt("spark.shuffle.sort.bypassMergeThreshold", 200)
    numPartitions <= bypassMergeThreshold && aggregator.isEmpty && keyOrdering.isEmpty
  }
}
