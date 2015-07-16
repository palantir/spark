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

import java.util

import org.apache.spark.SparkEnv
import org.apache.spark.SparkConf
import org.apache.spark.util.collection.{AppendOnlyMap, ExternalAppendOnlyMap}
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.MutableList
import scala.collection.Iterator

/**
 * Created by vladio on 7/14/15.
 */
private[spark] class ShuffleAggregationManager[K, V](
  val conf: SparkConf,
  iterator: Iterator[Product2[K, V]]) {

  private val isSpillEnabled = conf.getBoolean("spark.shuffle.spill", true)
  private val partialAggCheckInterval = conf.getInt("spark.partialAgg.interval", 10000)
  private val partialAggReduction = conf.getDouble("spark.partialAgg.reduction", 0.5)
  private var partialAggEnabled = true

  private val uniqueKeysMap = new AppendOnlyMap[K, Boolean]
  private var iteratedElements = MutableList[Product2[K, V]]()
  private var numIteratedRecords = 0

  def getRestoredIterator(): Iterator[Product2[K, V]] = {
    if (iterator.hasNext)
      iteratedElements.toIterator ++ iterator
    else
      iteratedElements.toIterator
  }

  def enableAggregation(): Boolean = {
    var ok : Boolean = true
    while (iterator.hasNext && partialAggEnabled && ok) {
      val kv = iterator.next()

      iteratedElements += kv
      numIteratedRecords += 1

      uniqueKeysMap.update(kv._1, true)

      if (numIteratedRecords == partialAggCheckInterval) {
        val partialAggSize = uniqueKeysMap.size
        if (partialAggSize > numIteratedRecords * partialAggReduction) {
          partialAggEnabled = false
        }

        ok = false
      }
    }

    partialAggEnabled
  }
}
