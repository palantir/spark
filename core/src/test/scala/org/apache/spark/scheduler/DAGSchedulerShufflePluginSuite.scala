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
package org.apache.spark.scheduler

import java.util.{Collections, Map => JMap}

import org.apache.spark.{FetchFailed, HashPartitioner, ShuffleDependency, SparkConf, Success}
import org.apache.spark.rdd.RDD
import org.apache.spark.shuffle.api.ShuffleDriverComponents
import org.apache.spark.storage.BlockManagerId

class PluginShuffleDriverComponents extends ShuffleDriverComponents {

  override def initializeApplication(): JMap[String, String] = Collections.emptyMap()

  override def unregisterOutputOnHostOnFetchFailure(): Boolean = true
}

class DAGSchedulerShufflePluginSuite extends DAGSchedulerSuite {

  private def setupTest(): (RDD[_], Int) = {
    afterEach()
    val conf = new SparkConf()
    // unregistering all outputs on a host is enabled for the individual file server case
    init(conf, (_, _) => new PluginShuffleDriverComponents)
    val shuffleMapRdd = new MyRDD(sc, 2, Nil)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, new HashPartitioner(2))
    val shuffleId = shuffleDep.shuffleId
    val reduceRdd = new MyRDD(sc, 2, List(shuffleDep), tracker = mapOutputTracker)
    (reduceRdd, shuffleId)
  }

  test("Test simple file server") {
    val (reduceRdd, shuffleId) = setupTest()
    submit(reduceRdd, Array(0, 1))

    // Perform map task
    val mapStatus1 = makeMapStatus(null, "hostA")
    val mapStatus2 = makeMapStatus(null, "hostB")
    complete(taskSets(0), Seq((Success, mapStatus1), (Success, mapStatus2)))
    assertMapShuffleLocations(shuffleId, Seq(mapStatus1, mapStatus2))

    // perform reduce task
    complete(taskSets(1), Seq((Success, 42), (Success, 43)))
    assert(results === Map(0 -> 42, 1 -> 43))
    assertDataStructuresEmpty()
  }

  test("Test simple file server fetch failure") {
    val (reduceRdd, shuffleId) = setupTest()
    submit(reduceRdd, Array(0, 1))

    // Perform map task
    val mapStatus1 = makeMapStatus(null, "hostA")
    val mapStatus2 = makeMapStatus(null, "hostB")
    complete(taskSets(0), Seq((Success, mapStatus1), (Success, mapStatus2)))

    complete(taskSets(1), Seq((Success, 42),
      (FetchFailed(BlockManagerId(null, "hostB", 1234), shuffleId, 1, 0, "ignored"), null)))
    assertMapShuffleLocations(shuffleId, Seq(mapStatus1, null))

    scheduler.resubmitFailedStages()
    complete(taskSets(2), Seq((Success, mapStatus2)))

    complete(taskSets(3), Seq((Success, 43)))
    assert(results === Map(0 -> 42, 1 -> 43))
    assertDataStructuresEmpty()
  }

  test("Test simple file fetch server - duplicate host") {
    val (reduceRdd, shuffleId) = setupTest()
    submit(reduceRdd, Array(0, 1))

    // Perform map task
    val mapStatus1 = makeMapStatus(null, "hostA")
    val mapStatus2 = makeMapStatus(null, "hostA")
    complete(taskSets(0), Seq((Success, mapStatus1), (Success, mapStatus2)))

    complete(taskSets(1), Seq((Success, 42),
      (FetchFailed(BlockManagerId(null, "hostA", 1234), shuffleId, 1, 0, "ignored"), null)))
    assertMapShuffleLocations(shuffleId, Seq(null, null)) // removes both

    scheduler.resubmitFailedStages()
    complete(taskSets(2), Seq((Success, mapStatus1), (Success, mapStatus2)))

    complete(taskSets(3), Seq((Success, 43)))
    assert(results === Map(0 -> 42, 1 -> 43))
    assertDataStructuresEmpty()
  }

  test("Test DFS case - empty BlockManagerId") {
    val (reduceRdd, shuffleId) = setupTest()
    submit(reduceRdd, Array(0, 1))

    val mapStatus = makeEmptyMapStatus()
    complete(taskSets(0), Seq((Success, mapStatus), (Success, mapStatus)))
    assertMapShuffleLocations(shuffleId, Seq(mapStatus, mapStatus))

    // perform reduce task
    complete(taskSets(1), Seq((Success, 42), (Success, 43)))
    assert(results === Map(0 -> 42, 1 -> 43))
    assertDataStructuresEmpty()
  }

  test("Test DFS case - fetch failure") {
    val (reduceRdd, shuffleId) = setupTest()
    submit(reduceRdd, Array(0, 1))

    // Perform map task
    val mapStatus = makeEmptyMapStatus()
    complete(taskSets(0), Seq((Success, mapStatus), (Success, mapStatus)))

    complete(taskSets(1), Seq((Success, 42),
      (FetchFailed(null, shuffleId, 1, 0, "ignored"), null)))
    assertMapShuffleLocations(shuffleId, Seq(mapStatus, null))

    scheduler.resubmitFailedStages()
    complete(taskSets(2), Seq((Success, mapStatus)))

    complete(taskSets(3), Seq((Success, 43)))
    assert(results === Map(0 -> 42, 1 -> 43))
    assertDataStructuresEmpty()
  }

  def makeMapStatus(execId: String, host: String): MapStatus = {
    MapStatus(BlockManagerId(execId, host, 1234), Array.fill[Long](2)(2), 0)
  }

  def makeEmptyMapStatus(): MapStatus = {
    MapStatus(null, Array.fill[Long](2)(2), 0)
  }

  def assertMapShuffleLocations(shuffleId: Int, set: Seq[MapStatus]): Unit = {
    val actualShuffleLocations = mapOutputTracker.shuffleStatuses(shuffleId).mapStatuses
    assert(actualShuffleLocations.toSeq === set)
  }
}
