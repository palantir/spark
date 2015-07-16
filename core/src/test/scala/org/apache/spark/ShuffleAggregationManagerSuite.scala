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

package org.apache.spark

import org.apache.spark.shuffle.ShuffleAggregationManager
import org.apache.spark.shuffle.sort.SortShuffleWriter._
import org.mockito.Mockito._

/**
 * Created by vladio on 7/15/15.
 */
class ShuffleAggregationManagerSuite extends SparkFunSuite {

  test("conditions for doing the pre-aggregation") {
    val conf = new SparkConf(loadDefaults = false)
    conf.set("spark.partialAgg.interval", "4")
    conf.set("spark.partialAgg.reduction", "0.5")

    // This test will pass if the first 4 elements of a set contains at most 2 unique keys.
    // Generate the records.
    val records = Iterator((1, "Vlad"), (2, "Marius"), (1, "Marian"), (2, "Cornel"), (3, "Patricia"), (4, "Georgeta"))

    // Test.
    val aggManager = new ShuffleAggregationManager[Int, String](conf, records)
    assert(aggManager.enableAggregation() == true)
  }

  test("conditions for skipping the pre-aggregation") {
    val conf = new SparkConf(loadDefaults = false)
    conf.set("spark.partialAgg.interval", "4")
    conf.set("spark.partialAgg.reduction", "0.5")

    val records = Iterator((1, "Vlad"), (2, "Marius"), (3, "Marian"), (2, "Cornel"), (3, "Patricia"), (4, "Georgeta"))

    val aggManager = new ShuffleAggregationManager[Int, String](conf, records)
    assert(aggManager.enableAggregation() == false)
  }

  test("restoring the iterator") {
    val conf = new SparkConf(loadDefaults = false)
    conf.set("spark.partialAgg.interval", "4")
    conf.set("spark.partialAgg.reduction", "0.5")

    val listOfElements = List((1, "Vlad"), (2, "Marius"), (1, "Marian"), (2, "Cornel"), (3, "Patricia"), (4, "Georgeta"))
    val records = listOfElements.toIterator
    val recordsCopy = listOfElements.toIterator

    val aggManager = new ShuffleAggregationManager[Int, String](conf, records)
    assert(aggManager.enableAggregation() == true)

    val restoredRecords = aggManager.getRestoredIterator()
    assert(restoredRecords.hasNext)

    while (restoredRecords.hasNext && recordsCopy.hasNext) {
      val kv1 = restoredRecords.next()
      val kv2 = recordsCopy.next()

      assert(kv1 == kv2)
    }

    assert(!restoredRecords.hasNext)
    assert(!recordsCopy.hasNext)
  }
}
