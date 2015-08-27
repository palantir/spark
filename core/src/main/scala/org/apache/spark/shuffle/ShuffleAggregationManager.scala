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


import org.apache.spark.SparkConf
import org.apache.spark.SparkEnv
import org.apache.spark.util.collection.{SizeTrackingVector, AppendOnlyMap}
import scala.collection.Iterator

private[spark] class ShuffleAggregationManager[K, V](
  val conf: SparkConf,
  records: Iterator[Product2[K, V]]) {

  private val partialAggCheckInterval = conf.getInt("spark.partialAgg.interval", 10000)
  private val partialAggReduction = conf.getDouble("spark.partialAgg.reduction", 0.5)
  private val partialAggInMemoryThreshold =
    SparkEnv.get.conf.getLong("spark.shuffle.spill.initialMemoryThreshold", 5 * 1024 * 1024)
  private val shuffleMemoryManager = SparkEnv.get.shuffleMemoryManager
  private var partialAggEnabled = true

  private val uniqueKeysMap = new AppendOnlyMap[K, Boolean]
  private var iteratedElements = new SizeTrackingVector[Product2[K, V]]()
  private var numIteratedRecords = 0

  def getRestoredIterator(): Iterator[Product2[K, V]] = {
    if (records.hasNext) {
      iteratedElements.iterator ++ records
    } else {
      iteratedElements.iterator
    }
  }

  def enableAggregation(): Boolean = {
    while (records.hasNext
        && numIteratedRecords < partialAggCheckInterval
        && partialAggEnabled) {
      val kv = records.next()

      iteratedElements += kv
      numIteratedRecords += 1

      uniqueKeysMap.update(kv._1, true)

      shuffleMemoryManager.getMemoryConsumptionForThisTask()

      if (iteratedElements.estimateSize() > partialAggInMemoryThreshold ||
          numIteratedRecords == partialAggCheckInterval) {
        val partialAggSize = uniqueKeysMap.size
        if (partialAggSize > numIteratedRecords * partialAggReduction) {
          partialAggEnabled = false
        }
      }
    }

    partialAggEnabled
  }
}
