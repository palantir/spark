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

import java.io.{File, FileOutputStream}

import com.google.common.io.CountingOutputStream
import org.apache.commons.io.FileUtils
import org.mockito.{Mock, MockitoAnnotations}
import org.mockito.Answers.RETURNS_SMART_NULLS
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
import scala.util.Random

import org.apache.spark.{Aggregator, MapOutputTracker, ShuffleDependency, SparkConf, SparkEnv, TaskContext}
import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.memory.{TaskMemoryManager, TestMemoryManager}
import org.apache.spark.metrics.source.Source
import org.apache.spark.network.BlockTransferService
import org.apache.spark.network.buffer.{FileSegmentManagedBuffer, ManagedBuffer}
import org.apache.spark.network.netty.{NettyBlockTransferService, SparkTransportConf}
import org.apache.spark.network.util.TransportConf
import org.apache.spark.rpc.{RpcEndpoint, RpcEndpointRef, RpcEnv}
import org.apache.spark.serializer.{KryoSerializer, SerializerManager}
import org.apache.spark.shuffle.{BaseShuffleHandle, BlockStoreShuffleReader, FetchFailedException}
import org.apache.spark.storage.{BlockId, BlockManager, BlockManagerId, BlockManagerMaster, ShuffleBlockId}
import org.apache.spark.util.{AccumulatorV2, TaskCompletionListener, TaskFailureListener, Utils}

/**
 * Benchmark to measure performance for aggregate primitives.
 * {{{
 *   To run this benchmark:
 *   1. without sbt: bin/spark-submit --class <this class> <spark sql test jar>
 *   2. build/sbt "sql/test:runMain <this class>"
 *   3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/test:runMain <this class>"
 *      Results will be written to "benchmarks/<this class>-results.txt".
 * }}}
 */
object BlockStoreShuffleReaderBenchmark extends BenchmarkBase {

  // this is only used to retrieve the aggregator/sorters/serializers,
  // so it shouldn't affect the performance significantly
  @Mock(answer = RETURNS_SMART_NULLS) private var dependency:
    ShuffleDependency[String, String, String] = _
  // only used to retrieve info about the maps at the beginning, doesn't affect perf
  @Mock(answer = RETURNS_SMART_NULLS) private var mapOutputTracker: MapOutputTracker = _
  // this is only used when initializing the BlockManager, so doesn't affect perf
  @Mock(answer = RETURNS_SMART_NULLS) private var blockManagerMaster: BlockManagerMaster = _
  // this is only used when initiating the BlockManager, for comms between master and executor
  @Mock(answer = RETURNS_SMART_NULLS) private var rpcEnv: RpcEnv = _
  @Mock(answer = RETURNS_SMART_NULLS) protected var rpcEndpointRef: RpcEndpointRef = _

  private var tempDir: File = _

  private val SHUFFLE_ID: Int = 0
  private val REDUCE_ID: Int = 0
  private val NUM_MAPS: Int = 5
  private val DEFAULT_DATA_STRING_SIZE = 5
  private val TEST_DATA_SIZE: Int = 10000000
  private val SMALLER_DATA_SIZE: Int = 2000000
  private val MIN_NUM_ITERS: Int = 10

  private val executorId: String = "0"
  private val localPort: Int = 17000
  private val remotePort: Int = 17002

  private val defaultConf: SparkConf = new SparkConf()
    .set("spark.shuffle.compress", "false")
    .set("spark.shuffle.spill.compress", "false")
    .set("spark.authenticate", "false")
    .set("spark.app.id", "test-app")
  private val serializer: KryoSerializer = new KryoSerializer(defaultConf)
  private val serializerManager: SerializerManager = new SerializerManager(serializer, defaultConf)
  private val execBlockManagerId: BlockManagerId =
    BlockManagerId(executorId, "localhost", localPort)
  private val remoteBlockManagerId: BlockManagerId =
    BlockManagerId(executorId, "localhost", remotePort)
  private val transportConf: TransportConf =
    SparkTransportConf.fromSparkConf(defaultConf, "shuffle")
  private val securityManager: org.apache.spark.SecurityManager =
    new org.apache.spark.SecurityManager(defaultConf)
  protected val memoryManager: TestMemoryManager = new TestMemoryManager(defaultConf)

  class TestBlockManager(transferService: BlockTransferService,
      blockManagerMaster: BlockManagerMaster,
      dataFile: File,
      fileLength: Long) extends BlockManager(
    executorId,
    rpcEnv,
    blockManagerMaster,
    serializerManager,
    defaultConf,
    memoryManager,
    null,
    null,
    transferService,
    null,
    1) {
    blockManagerId = execBlockManagerId

    override def getBlockData(blockId: BlockId): ManagedBuffer = {
      new FileSegmentManagedBuffer(
        transportConf,
        dataFile,
        0,
        fileLength
      )
    }
  }

  private var blockManager : BlockManager = _
  private var externalBlockManager: BlockManager = _

  def getTestBlockManager(port: Int, dataFile: File, dataFileLength: Long): TestBlockManager = {
    val shuffleClient = new NettyBlockTransferService(
      defaultConf,
      securityManager,
      "localhost",
      "localhost",
      port,
      1
    )
    new TestBlockManager(shuffleClient,
      blockManagerMaster,
      dataFile,
      dataFileLength)
  }

  def initializeServers(dataFile: File, dataFileLength: Long): Unit = {
    MockitoAnnotations.initMocks(this)
    when(blockManagerMaster.registerBlockManager(
      any[BlockManagerId], any[Long], any[Long], any[RpcEndpointRef])).thenReturn(null)
    when(rpcEnv.setupEndpoint(any[String], any[RpcEndpoint])).thenReturn(rpcEndpointRef)
    blockManager = getTestBlockManager(localPort, dataFile, dataFileLength)
    blockManager.initialize(defaultConf.getAppId)
    externalBlockManager = getTestBlockManager(remotePort, dataFile, dataFileLength)
    externalBlockManager.initialize(defaultConf.getAppId)
  }

  def stopServers(): Unit = {
    blockManager.stop()
    externalBlockManager.stop()
  }

  def setupReader(
      dataFile: File,
      dataFileLength: Long,
      fetchLocal: Boolean,
      aggregator: Option[Aggregator[String, String, String]] = None,
      sorter: Option[Ordering[String]] = None): BlockStoreShuffleReader[String, String] = {
    SparkEnv.set(new SparkEnv(
      "0",
      null,
      serializer,
      null,
      serializerManager,
      mapOutputTracker,
      null,
      null,
      blockManager,
      null,
      null,
      null,
      null,
      defaultConf
    ))

    val shuffleHandle = new BaseShuffleHandle(
      shuffleId = SHUFFLE_ID,
      numMaps = NUM_MAPS,
      dependency = dependency)

    // We cannot mock the TaskContext because it taskMetrics() gets called at every next()
    // call on the reader, and Mockito will try to log all calls to taskMetrics(), thus OOM-ing
    // the test
    val taskContext = new TaskContext {
      private val metrics: TaskMetrics = new TaskMetrics
      private val testMemManager = new TestMemoryManager(defaultConf)
      private val taskMemManager = new TaskMemoryManager(testMemManager, 0)
      testMemManager.limit(PackedRecordPointer.MAXIMUM_PAGE_SIZE_BYTES)
      override def isCompleted(): Boolean = false
      override def isInterrupted(): Boolean = false
      override def addTaskCompletionListener(listener: TaskCompletionListener):
        TaskContext = { null }
      override def addTaskFailureListener(listener: TaskFailureListener): TaskContext = { null }
      override def stageId(): Int = 0
      override def stageAttemptNumber(): Int = 0
      override def partitionId(): Int = 0
      override def attemptNumber(): Int = 0
      override def taskAttemptId(): Long = 0
      override def getLocalProperty(key: String): String = ""
      override def taskMetrics(): TaskMetrics = metrics
      override def getMetricsSources(sourceName: String): Seq[Source] = Seq.empty
      override private[spark] def killTaskIfInterrupted(): Unit = {}
      override private[spark] def getKillReason() = None
      override private[spark] def taskMemoryManager() = taskMemManager
      override private[spark] def registerAccumulator(a: AccumulatorV2[_, _]): Unit = {}
      override private[spark] def setFetchFailed(fetchFailed: FetchFailedException): Unit = {}
      override private[spark] def markInterrupted(reason: String): Unit = {}
      override private[spark] def markTaskFailed(error: Throwable): Unit = {}
      override private[spark] def markTaskCompleted(error: Option[Throwable]): Unit = {}
      override private[spark] def fetchFailed = None
      override private[spark] def getLocalProperties = { null }
    }
    TaskContext.setTaskContext(taskContext)

    var dataBlockId: BlockManagerId = execBlockManagerId
    if (!fetchLocal) {
      dataBlockId = remoteBlockManagerId
    }

    when(mapOutputTracker.getMapSizesByExecutorId(SHUFFLE_ID, REDUCE_ID, REDUCE_ID + 1))
      .thenReturn {
        val shuffleBlockIdsAndSizes = (0 until NUM_MAPS).map { mapId =>
          val shuffleBlockId = ShuffleBlockId(SHUFFLE_ID, mapId, REDUCE_ID)
          (shuffleBlockId, dataFileLength)
        }
        Seq((dataBlockId, shuffleBlockIdsAndSizes)).toIterator
      }

    when(dependency.serializer).thenReturn(serializer)
    when(dependency.aggregator).thenReturn(aggregator)
    when(dependency.keyOrdering).thenReturn(sorter)

    new BlockStoreShuffleReader[String, String](
      shuffleHandle,
      0,
      1,
      taskContext,
      taskContext.taskMetrics().createTempShuffleReadMetrics(),
      serializerManager,
      blockManager,
      mapOutputTracker
    )
  }

  def generateDataOnDisk(size: Int, file: File): Long = {
    // scalastyle:off println
    println("Generating test data with num records: " + size)
    val random = new Random(123)
    val dataOutput = new FileOutputStream(file)
    val coutingOutput = new CountingOutputStream(dataOutput)
    val serializedOutput = serializer.newInstance().serializeStream(coutingOutput)
    try {
      (1 to size).foreach { i => {
        if (i % 1000000 == 0) {
          println("Wrote " + i + " test data points")
        }
        val x = random.alphanumeric.take(DEFAULT_DATA_STRING_SIZE).mkString
        serializedOutput.writeKey(x)
        serializedOutput.writeValue(x)
      }}
    }
    finally {
      serializedOutput.close()
    }
    coutingOutput.getCount
    // scalastyle:off println
  }

  def runWithLargeDataset(): Unit = {
    val size = TEST_DATA_SIZE
    val tempDataFile: File = File.createTempFile("test-data", "", tempDir)
    val dataFileLength = generateDataOnDisk(size, tempDataFile)
    val baseBenchmark =
      new Benchmark("no aggregation or sorting",
        size,
        minNumIters = MIN_NUM_ITERS,
        output = output,
        outputPerIteration = true)
    baseBenchmark.addTimerCase("local fetch") { timer =>
      val reader = setupReader(tempDataFile, dataFileLength, fetchLocal = true)
      timer.startTiming()
      val numRead = reader.read().length
      timer.stopTiming()
      assert(numRead == size * NUM_MAPS)
    }
    baseBenchmark.addTimerCase("remote rpc fetch") { timer =>
      val reader = setupReader(tempDataFile, dataFileLength, fetchLocal = false)
      timer.startTiming()
      val numRead = reader.read().length
      timer.stopTiming()
      assert(numRead == size * NUM_MAPS)
    }
    baseBenchmark.run()
    tempDataFile.delete()
    stopServers()
  }

  def runWithSmallerDataset(): Unit = {
    val size = SMALLER_DATA_SIZE
    val smallerDataFile: File = File.createTempFile("test-data", "", tempDir)
    val smallerFileLength = generateDataOnDisk(size, smallerDataFile)
    initializeServers(smallerDataFile, smallerFileLength)

    def createCombiner(i: String): String = i
    def mergeValue(i: String, j: String): String = if (Ordering.String.compare(i, j) > 0) i else j
    def mergeCombiners(i: String, j: String): String =
      if (Ordering.String.compare(i, j) > 0) i else j
    val aggregator =
      new Aggregator[String, String, String](createCombiner, mergeValue, mergeCombiners)
    val aggregationBenchmark =
      new Benchmark("with aggregation",
        size,
        minNumIters = MIN_NUM_ITERS,
        output = output,
        outputPerIteration = true)
    aggregationBenchmark.addTimerCase("local fetch") { timer =>
      val reader = setupReader(
        smallerDataFile,
        smallerFileLength,
        fetchLocal = true,
        aggregator = Some(aggregator))
      timer.startTiming()
      val numRead = reader.read().length
      timer.stopTiming()
      assert(numRead > 0)
    }
    aggregationBenchmark.addTimerCase("remote rpc fetch") { timer =>
      val reader = setupReader(
        smallerDataFile,
        smallerFileLength,
        fetchLocal = false,
        aggregator = Some(aggregator))
      timer.startTiming()
      val numRead = reader.read().length
      timer.stopTiming()
      assert(numRead > 0)
    }
    aggregationBenchmark.run()


    val sorter = Ordering.String
    val sortingBenchmark =
      new Benchmark("with sorting",
        size,
        minNumIters = MIN_NUM_ITERS,
        output = output,
        outputPerIteration = true)
    sortingBenchmark.addTimerCase("local fetch") { timer =>
      val reader = setupReader(
        smallerDataFile,
        smallerFileLength,
        fetchLocal = true,
        sorter = Some(sorter))
      timer.startTiming()
      val numRead = reader.read().length
      timer.stopTiming()
      assert(numRead == size * NUM_MAPS)
    }
    sortingBenchmark.addTimerCase("remote rpc fetch") { timer =>
      val reader = setupReader(
        smallerDataFile,
        smallerFileLength,
        fetchLocal = false,
        sorter = Some(sorter))
      timer.startTiming()
      val numRead = reader.read().length
      timer.stopTiming()
      assert(numRead == size * NUM_MAPS)
    }
    sortingBenchmark.run()
    stopServers()
    smallerDataFile.delete()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    tempDir = Utils.createTempDir(null, "shuffle")

    runBenchmark("BlockStoreShuffleReader reader") {
      runWithLargeDataset()
      runWithSmallerDataset()
    }

    FileUtils.deleteDirectory(tempDir)
  }
}
