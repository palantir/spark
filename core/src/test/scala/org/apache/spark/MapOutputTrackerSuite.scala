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

import scala.collection.mutable.ArrayBuffer

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._

import org.apache.spark.LocalSparkContext._
import org.apache.spark.broadcast.BroadcastManager
import org.apache.spark.internal.config._
import org.apache.spark.internal.config.Network.{RPC_ASK_TIMEOUT, RPC_MESSAGE_MAX_SIZE}
import org.apache.spark.rpc.{RpcAddress, RpcCallContext, RpcEnv}
import org.apache.spark.scheduler.{CompressedMapStatus, MapStatus}
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.shuffle.api.ShuffleDriverComponents
import org.apache.spark.storage.{BlockManagerId, ShuffleBlockAttemptId}

class MapOutputTrackerSuite extends SparkFunSuite {
  private val conf = new SparkConf

  private def newTrackerMaster(sparkConf: SparkConf = conf) = {
    val broadcastManager = new BroadcastManager(true, sparkConf,
      new SecurityManager(sparkConf))
    val driverComponents = mock(classOf[ShuffleDriverComponents])
    when(driverComponents.checkIfMapOutputStoredOutsideExecutor(any(), any())).thenReturn(false)
    new MapOutputTrackerMaster(sparkConf, broadcastManager, true, driverComponents)
  }

  def createRpcEnv(name: String, host: String = "localhost", port: Int = 0,
      securityManager: SecurityManager = new SecurityManager(conf)): RpcEnv = {
    RpcEnv.create(name, host, port, conf, securityManager)
  }

  test("master start and stop") {
    val rpcEnv = createRpcEnv("test")
    val tracker = newTrackerMaster()
    tracker.trackerEndpoint = rpcEnv.setupEndpoint(MapOutputTracker.ENDPOINT_NAME,
      new MapOutputTrackerMasterEndpoint(rpcEnv, tracker, conf))
    tracker.stop()
    rpcEnv.shutdown()
  }

  test("master register shuffle and fetch") {
    val rpcEnv = createRpcEnv("test")
    val tracker = newTrackerMaster()
    tracker.trackerEndpoint = rpcEnv.setupEndpoint(MapOutputTracker.ENDPOINT_NAME,
      new MapOutputTrackerMasterEndpoint(rpcEnv, tracker, conf))
    tracker.registerShuffle(10, 2)
    assert(tracker.containsShuffle(10))
    val size1000 = MapStatus.decompressSize(MapStatus.compressSize(1000L))
    val size10000 = MapStatus.decompressSize(MapStatus.compressSize(10000L))
    tracker.registerMapOutput(10, 0, MapStatus(BlockManagerId("a", "hostA", 1000),
        Array(1000L, 10000L), 0))
    tracker.registerMapOutput(10, 1, MapStatus(BlockManagerId("b", "hostB", 1000),
        Array(10000L, 1000L), 0))
    val statuses = tracker.getMapSizesByExecutorId(10, 0)
    assert(statuses.map(status => (status._1.get, status._2)).toSet ===
      Seq((BlockManagerId("b", "hostB", 1000),
            ArrayBuffer((ShuffleBlockAttemptId(10, 1, 0, 0), size10000))),
          (BlockManagerId("a", "hostA", 1000),
            ArrayBuffer((ShuffleBlockAttemptId(10, 0, 0, 0), size1000))))
        .toSet)
    assert(0 == tracker.getNumCachedSerializedBroadcast)
    tracker.stop()
    rpcEnv.shutdown()
  }

  test("master register and unregister shuffle") {
    val rpcEnv = createRpcEnv("test")
    val tracker = newTrackerMaster()
    tracker.trackerEndpoint = rpcEnv.setupEndpoint(MapOutputTracker.ENDPOINT_NAME,
      new MapOutputTrackerMasterEndpoint(rpcEnv, tracker, conf))
    tracker.registerShuffle(10, 2)
    val compressedSize1000 = MapStatus.compressSize(1000L)
    val compressedSize10000 = MapStatus.compressSize(10000L)
    tracker.registerMapOutput(10, 0, MapStatus(BlockManagerId("a", "hostA", 1000),
      Array(compressedSize1000, compressedSize10000), 0))
    tracker.registerMapOutput(10, 1, MapStatus(BlockManagerId("b", "hostB", 1000),
      Array(compressedSize10000, compressedSize1000), 0))
    assert(tracker.containsShuffle(10))
    assert(tracker.getMapSizesByExecutorId(10, 0).nonEmpty)
    assert(0 == tracker.getNumCachedSerializedBroadcast)
    tracker.unregisterShuffle(10)
    assert(!tracker.containsShuffle(10))
    assert(tracker.getMapSizesByExecutorId(10, 0).isEmpty)

    tracker.stop()
    rpcEnv.shutdown()
  }

  test("master register shuffle and unregister map output and fetch") {
    val rpcEnv = createRpcEnv("test")
    val tracker = newTrackerMaster()
    tracker.trackerEndpoint = rpcEnv.setupEndpoint(MapOutputTracker.ENDPOINT_NAME,
      new MapOutputTrackerMasterEndpoint(rpcEnv, tracker, conf))
    tracker.registerShuffle(10, 2)
    val compressedSize1000 = MapStatus.compressSize(1000L)
    val compressedSize10000 = MapStatus.compressSize(10000L)
    tracker.registerMapOutput(10, 0, MapStatus(BlockManagerId("a", "hostA", 1000),
        Array(compressedSize1000, compressedSize1000, compressedSize1000), 0))
    tracker.registerMapOutput(10, 1, MapStatus(BlockManagerId("b", "hostB", 1000),
        Array(compressedSize10000, compressedSize1000, compressedSize1000), 0))

    assert(0 == tracker.getNumCachedSerializedBroadcast)
    // As if we had two simultaneous fetch failures
    tracker.unregisterMapOutput(10, 0, BlockManagerId("a", "hostA", 1000))
    tracker.unregisterMapOutput(10, 0, BlockManagerId("a", "hostA", 1000))

    // The remaining reduce task might try to grab the output despite the shuffle failure;
    // this should cause it to fail, and the scheduler will ignore the failure due to the
    // stage already being aborted.
    intercept[FetchFailedException] { tracker.getMapSizesByExecutorId(10, 1) }

    tracker.stop()
    rpcEnv.shutdown()
  }

  test("remote fetch") {
    val hostname = "localhost"
    val rpcEnv = createRpcEnv("spark", hostname, 0, new SecurityManager(conf))

    val masterTracker = newTrackerMaster()
    masterTracker.trackerEndpoint = rpcEnv.setupEndpoint(MapOutputTracker.ENDPOINT_NAME,
      new MapOutputTrackerMasterEndpoint(rpcEnv, masterTracker, conf))

    val slaveRpcEnv = createRpcEnv("spark-slave", hostname, 0, new SecurityManager(conf))
    val slaveTracker = new MapOutputTrackerWorker(conf)
    slaveTracker.trackerEndpoint =
      slaveRpcEnv.setupEndpointRef(rpcEnv.address, MapOutputTracker.ENDPOINT_NAME)

    masterTracker.registerShuffle(10, 1)
    slaveTracker.updateEpoch(masterTracker.getEpoch)
    // This is expected to fail because no outputs have been registered for the shuffle.
    intercept[FetchFailedException] { slaveTracker.getMapSizesByExecutorId(10, 0) }

    val size1000 = MapStatus.decompressSize(MapStatus.compressSize(1000L))
    masterTracker.registerMapOutput(10, 0, MapStatus(
      BlockManagerId("a", "hostA", 1000), Array(1000L), 0))
    slaveTracker.updateEpoch(masterTracker.getEpoch)
    assert(slaveTracker.getMapSizesByExecutorId(10, 0)
      .map(status => (status._1.get, status._2)).toSeq ===
      Seq((BlockManagerId("a", "hostA", 1000),
        ArrayBuffer((ShuffleBlockAttemptId(10, 0, 0, 0), size1000)))))
    assert(0 == masterTracker.getNumCachedSerializedBroadcast)

    val masterTrackerEpochBeforeLossOfMapOutput = masterTracker.getEpoch
    masterTracker.unregisterMapOutput(10, 0, BlockManagerId("a", "hostA", 1000))
    assert(masterTracker.getEpoch > masterTrackerEpochBeforeLossOfMapOutput)
    slaveTracker.updateEpoch(masterTracker.getEpoch)
    intercept[FetchFailedException] { slaveTracker.getMapSizesByExecutorId(10, 0) }

    // failure should be cached
    intercept[FetchFailedException] { slaveTracker.getMapSizesByExecutorId(10, 0) }
    assert(0 == masterTracker.getNumCachedSerializedBroadcast)

    masterTracker.stop()
    slaveTracker.stop()
    rpcEnv.shutdown()
    slaveRpcEnv.shutdown()
  }

  test("remote fetch below max RPC message size") {
    val newConf = new SparkConf
    newConf.set(RPC_MESSAGE_MAX_SIZE, 1)
    newConf.set(RPC_ASK_TIMEOUT, "1") // Fail fast
    newConf.set(SHUFFLE_MAPOUTPUT_MIN_SIZE_FOR_BROADCAST, 1048576L)

    val masterTracker = newTrackerMaster(newConf)
    val rpcEnv = createRpcEnv("spark")
    val masterEndpoint = new MapOutputTrackerMasterEndpoint(rpcEnv, masterTracker, newConf)
    masterTracker.trackerEndpoint =
      rpcEnv.setupEndpoint(MapOutputTracker.ENDPOINT_NAME, masterEndpoint)

    // Message size should be ~123B, and no exception should be thrown
    masterTracker.registerShuffle(10, 1)
    masterTracker.registerMapOutput(10, 0, MapStatus(
      BlockManagerId("88", "mph", 1000), Array.fill[Long](10)(0), 0))
    val senderAddress = RpcAddress("localhost", 12345)
    val rpcCallContext = mock(classOf[RpcCallContext])
    when(rpcCallContext.senderAddress).thenReturn(senderAddress)
    masterEndpoint.receiveAndReply(rpcCallContext)(GetMapOutputStatuses(10))
    // Default size for broadcast in this testsuite is set to -1 so should not cause broadcast
    // to be used.
    verify(rpcCallContext, timeout(30000)).reply(any())
    assert(0 == masterTracker.getNumCachedSerializedBroadcast)

    masterTracker.stop()
    rpcEnv.shutdown()
  }

  test("min broadcast size exceeds max RPC message size") {
    val newConf = new SparkConf
    newConf.set(RPC_MESSAGE_MAX_SIZE, 1)
    newConf.set(RPC_ASK_TIMEOUT, "1") // Fail fast
    newConf.set(SHUFFLE_MAPOUTPUT_MIN_SIZE_FOR_BROADCAST, Int.MaxValue.toLong)

    intercept[IllegalArgumentException] { newTrackerMaster(newConf) }
  }

  test("getLocationsWithLargestOutputs with multiple outputs in same machine") {
    val rpcEnv = createRpcEnv("test")
    val tracker = newTrackerMaster()
    tracker.trackerEndpoint = rpcEnv.setupEndpoint(MapOutputTracker.ENDPOINT_NAME,
      new MapOutputTrackerMasterEndpoint(rpcEnv, tracker, conf))
    // Setup 3 map tasks
    // on hostA with output size 2
    // on hostA with output size 2
    // on hostB with output size 3
    tracker.registerShuffle(10, 3)
    tracker.registerMapOutput(10, 0, MapStatus(BlockManagerId("a", "hostA", 1000),
        Array(2L), 0))
    tracker.registerMapOutput(10, 1, MapStatus(BlockManagerId("a", "hostA", 1000),
        Array(2L), 0))
    tracker.registerMapOutput(10, 2, MapStatus(BlockManagerId("b", "hostB", 1000),
        Array(3L), 0))

    // When the threshold is 50%, only host A should be returned as a preferred location
    // as it has 4 out of 7 bytes of output.
    val topLocs50 = tracker.getLocationsWithLargestOutputs(10, 0, 1, 0.5)
    assert(topLocs50.nonEmpty)
    assert(topLocs50.get.size === 1)
    assert(topLocs50.get.head === BlockManagerId("a", "hostA", 1000))

    // When the threshold is 20%, both hosts should be returned as preferred locations.
    val topLocs20 = tracker.getLocationsWithLargestOutputs(10, 0, 1, 0.2)
    assert(topLocs20.nonEmpty)
    assert(topLocs20.get.size === 2)
    assert(topLocs20.get.toSet ===
           Seq(BlockManagerId("a", "hostA", 1000), BlockManagerId("b", "hostB", 1000)).toSet)

    tracker.stop()
    rpcEnv.shutdown()
  }

  test("remote fetch using broadcast") {
    val newConf = new SparkConf
    newConf.set(RPC_MESSAGE_MAX_SIZE, 1)
    newConf.set(RPC_ASK_TIMEOUT, "1") // Fail fast
    newConf.set(SHUFFLE_MAPOUTPUT_MIN_SIZE_FOR_BROADCAST, 10240L) // 10 KiB << 1MiB framesize

    // needs TorrentBroadcast so need a SparkContext
    withSpark(new SparkContext("local", "MapOutputTrackerSuite", newConf)) { sc =>
      val masterTracker = sc.env.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
      val rpcEnv = sc.env.rpcEnv
      val masterEndpoint = new MapOutputTrackerMasterEndpoint(rpcEnv, masterTracker, newConf)
      rpcEnv.stop(masterTracker.trackerEndpoint)
      rpcEnv.setupEndpoint(MapOutputTracker.ENDPOINT_NAME, masterEndpoint)

      // Frame size should be ~1.1MB, and MapOutputTrackerMasterEndpoint should throw exception.
      // Note that the size is hand-selected here because map output statuses are compressed before
      // being sent.
      masterTracker.registerShuffle(20, 100)
      (0 until 100).foreach { i =>
        masterTracker.registerMapOutput(20, i, new CompressedMapStatus(
          BlockManagerId("999", "mps", 1000), Array.fill[Long](4000000)(0), 0))
      }
      val senderAddress = RpcAddress("localhost", 12345)
      val rpcCallContext = mock(classOf[RpcCallContext])
      when(rpcCallContext.senderAddress).thenReturn(senderAddress)
      masterEndpoint.receiveAndReply(rpcCallContext)(GetMapOutputStatuses(20))
      // should succeed since majority of data is broadcast and actual serialized
      // message size is small
      verify(rpcCallContext, timeout(30000)).reply(any())
      assert(1 == masterTracker.getNumCachedSerializedBroadcast)
      masterTracker.unregisterShuffle(20)
      assert(0 == masterTracker.getNumCachedSerializedBroadcast)
    }
  }

  test("equally divide map statistics tasks") {
    val func = newTrackerMaster().equallyDivide _
    val cases = Seq((0, 5), (4, 5), (15, 5), (16, 5), (17, 5), (18, 5), (19, 5), (20, 5))
    val expects = Seq(
      Seq(0, 0, 0, 0, 0),
      Seq(1, 1, 1, 1, 0),
      Seq(3, 3, 3, 3, 3),
      Seq(4, 3, 3, 3, 3),
      Seq(4, 4, 3, 3, 3),
      Seq(4, 4, 4, 3, 3),
      Seq(4, 4, 4, 4, 3),
      Seq(4, 4, 4, 4, 4))
    cases.zip(expects).foreach { case ((num, divisor), expect) =>
      val answer = func(num, divisor).toSeq
      var wholeSplit = (0 until num)
      answer.zip(expect).foreach { case (split, expectSplitLength) =>
        val (currentSplit, rest) = wholeSplit.splitAt(expectSplitLength)
        assert(currentSplit.toSet == split.toSet)
        wholeSplit = rest
      }
    }
  }

  test("zero-sized blocks should be excluded when getMapSizesByExecutorId") {
    val rpcEnv = createRpcEnv("test")
    val tracker = newTrackerMaster()
    tracker.trackerEndpoint = rpcEnv.setupEndpoint(MapOutputTracker.ENDPOINT_NAME,
      new MapOutputTrackerMasterEndpoint(rpcEnv, tracker, conf))
    tracker.registerShuffle(10, 2)

    val size0 = MapStatus.decompressSize(MapStatus.compressSize(0L))
    val size1000 = MapStatus.decompressSize(MapStatus.compressSize(1000L))
    val size10000 = MapStatus.decompressSize(MapStatus.compressSize(10000L))
    tracker.registerMapOutput(10, 0, MapStatus(BlockManagerId("a", "hostA", 1000),
      Array(size0, size1000, size0, size10000), 0))
    tracker.registerMapOutput(10, 1, MapStatus(BlockManagerId("b", "hostB", 1000),
      Array(size10000, size0, size1000, size0), 0))
    assert(tracker.containsShuffle(10))
    assert(tracker.getMapSizesByExecutorId(10, 0, 4).toSeq ===
        Seq(
          (Some(BlockManagerId("b", "hostB", 1000)),
            Seq((ShuffleBlockAttemptId(10, 1, 0, 0), size10000),
                (ShuffleBlockAttemptId(10, 1, 2, 0), size1000))),
          (Some(BlockManagerId("a", "hostA", 1000)),
              Seq((ShuffleBlockAttemptId(10, 0, 1, 0), size1000),
                  (ShuffleBlockAttemptId(10, 0, 3, 0), size10000)))
        )
    )

    tracker.unregisterShuffle(10)
    tracker.stop()
    rpcEnv.shutdown()
  }

  test("shuffle map statuses with null blockManagerIds") {
    val rpcEnv = createRpcEnv("test")
    val tracker = newTrackerMaster()
    tracker.trackerEndpoint = rpcEnv.setupEndpoint(MapOutputTracker.ENDPOINT_NAME,
      new MapOutputTrackerMasterEndpoint(rpcEnv, tracker, conf))
    tracker.registerShuffle(10, 3)
    assert(tracker.containsShuffle(10))
    val size1000 = MapStatus.decompressSize(MapStatus.compressSize(1000L))
    val size10000 = MapStatus.decompressSize(MapStatus.compressSize(10000L))
    tracker.registerMapOutput(10, 0, MapStatus(BlockManagerId("a", "hostA", 1000),
      Array(1000L, 10000L), 0))
    tracker.registerMapOutput(10, 1, MapStatus(null, Array(10000L, 1000L), 0))
    tracker.registerMapOutput(10, 2, MapStatus(null, Array(1000L, 10000L), 0))
    var statuses = tracker.getMapSizesByExecutorId(10, 0)
    assert(statuses.toSet ===
      Seq(
        (None,
          ArrayBuffer((ShuffleBlockAttemptId(10, 1, 0, 0), size10000),
            (ShuffleBlockAttemptId(10, 2, 0, 0), size1000))),
        (Some(BlockManagerId("a", "hostA", 1000)),
          ArrayBuffer((ShuffleBlockAttemptId(10, 0, 0, 0), size1000))))
        .toSet)
    assert(0 == tracker.getNumCachedSerializedBroadcast)
    tracker.removeOutputsOnHost("hostA")

    tracker.registerMapOutput(10, 0, MapStatus(BlockManagerId("b", "hostB", 1000),
      Array(1000L, 10000L), 0))
    statuses = tracker.getMapSizesByExecutorId(10, 0)
    assert(statuses.toSet ===
      Seq(
        (None,
          ArrayBuffer((ShuffleBlockAttemptId(10, 1, 0, 0), size10000),
            (ShuffleBlockAttemptId(10, 2, 0, 0), size1000))),
        (Some(BlockManagerId("b", "hostB", 1000)),
          ArrayBuffer((ShuffleBlockAttemptId(10, 0, 0, 0), size1000))))
        .toSet)
    tracker.unregisterMapOutput(10, 1, null)

    tracker.registerMapOutput(10, 1, MapStatus(BlockManagerId("b", "hostB", 1000),
      Array(1000L, 10000L), 0))
    statuses = tracker.getMapSizesByExecutorId(10, 0)
    assert(statuses.toSet ===
      Seq(
        (Some(BlockManagerId("b", "hostB", 1000)),
          ArrayBuffer((ShuffleBlockAttemptId(10, 0, 0, 0), size1000),
                      (ShuffleBlockAttemptId(10, 1, 0, 0), size1000))),
        (None,
          ArrayBuffer((ShuffleBlockAttemptId(10, 2, 0, 0), size1000))))
        .toSet)

    val outputs = tracker.getLocationsWithLargestOutputs(10, 0, 2, 0.01)
    assert(outputs.get.toSeq === Seq(BlockManagerId("b", "hostB", 1000)))
    tracker.stop()
    rpcEnv.shutdown()
  }

  test("shuffle map statuses with null execIds") {
    val rpcEnv = createRpcEnv("test")
    val tracker = newTrackerMaster()
    tracker.trackerEndpoint = rpcEnv.setupEndpoint(MapOutputTracker.ENDPOINT_NAME,
      new MapOutputTrackerMasterEndpoint(rpcEnv, tracker, conf))
    tracker.registerShuffle(10, 2)
    assert(tracker.containsShuffle(10))
    val size1000 = MapStatus.decompressSize(MapStatus.compressSize(1000L))
    val size10000 = MapStatus.decompressSize(MapStatus.compressSize(10000L))
    tracker.registerMapOutput(10, 0, MapStatus(BlockManagerId(null, "hostA", 1000),
      Array(1000L, 10000L), 0))
    tracker.registerMapOutput(10, 1, MapStatus(BlockManagerId(null, "hostB", 1000),
      Array(10000L, 1000L), 0))
    var statuses = tracker.getMapSizesByExecutorId(10, 0)
    assert(statuses.toSet ===
      Seq(
        (Some(BlockManagerId(null, "hostB", 1000)),
          ArrayBuffer((ShuffleBlockAttemptId(10, 1, 0, 0), size10000))),
        (Some(BlockManagerId(null, "hostA", 1000)),
          ArrayBuffer((ShuffleBlockAttemptId(10, 0, 0, 0), size1000))))
        .toSet)
    assert(0 == tracker.getNumCachedSerializedBroadcast)
    tracker.removeOutputsOnExecutor("a")

    statuses = tracker.getMapSizesByExecutorId(10, 0)
    assert(statuses.toSet ===
      Seq(
        (Some(BlockManagerId(null, "hostB", 1000)),
          ArrayBuffer((ShuffleBlockAttemptId(10, 1, 0, 0), size10000))),
        (Some(BlockManagerId(null, "hostA", 1000)),
          ArrayBuffer((ShuffleBlockAttemptId(10, 0, 0, 0), size1000))))
        .toSet)
    tracker.unregisterMapOutput(10, 1, BlockManagerId(null, "hostA", 1000))

    tracker.registerMapOutput(10, 0, MapStatus(BlockManagerId("b", "hostB", 1000),
      Array(1000L, 10000L), 0))
    statuses = tracker.getMapSizesByExecutorId(10, 0)
    assert(statuses.toSet ===
      Seq(
        (Some(BlockManagerId(null, "hostB", 1000)),
          ArrayBuffer((ShuffleBlockAttemptId(10, 1, 0, 0), size10000))),
        (Some(BlockManagerId("b", "hostB", 1000)),
          ArrayBuffer((ShuffleBlockAttemptId(10, 0, 0, 0), size1000))))
        .toSet)
    val outputs = tracker.getLocationsWithLargestOutputs(10, 0, 2, 0.01)
    assert(outputs.get.toSeq === Seq(BlockManagerId("b", "hostB", 1000)))
    tracker.stop()
    rpcEnv.shutdown()
  }

  test("correctly track executors and ExecutorShuffleStatus") {
    val tracker = newTrackerMaster()
    val bmId1 = BlockManagerId("exec1", "host1", 1000)
    val bmId2 = BlockManagerId("exec2", "host2", 1000)
    tracker.registerShuffle(11, 3)
    tracker.registerMapOutput(11, 0, MapStatus(bmId1, Array(10), 0L))
    tracker.registerMapOutput(11, 1, MapStatus(bmId1, Array(100), 1L))
    tracker.registerMapOutput(11, 2, MapStatus(bmId2, Array(1000), 2L))

    assert(tracker.hasOutputsOnExecutor("exec1"))
    assert(tracker.getExecutorShuffleStatus.keySet.equals(Set("exec1", "exec2")))
    assert(!tracker.hasOutputsOnExecutor("exec3"))

    tracker.unregisterMapOutput(11, 0, bmId1)
    assert(tracker.hasOutputsOnExecutor("exec1"))
    tracker.unregisterMapOutput(11, 1, bmId1)
    assert(!tracker.hasOutputsOnExecutor("exec1"))

    tracker.markShuffleInactive(11)
    assert(tracker.hasOutputsOnExecutor("exec2"))
    assert(!tracker.hasOutputsOnExecutor("exec2", activeOnly = true))

    assert(tracker.getExecutorShuffleStatus == Map("exec2" -> ExecutorShuffleStatus.Inactive))
    tracker.markShuffleActive(11)
    assert(tracker.getExecutorShuffleStatus == Map("exec2" -> ExecutorShuffleStatus.Active))
  }
}
