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
      fileLength: Long,
      offset: Long) extends BlockManager(
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
        offset,
        fileLength
      )
    }
  }

  private var blockManager : BlockManager = _
  private var externalBlockManager: BlockManager = _

  def getTestBlockManager(
      port: Int,
      dataFile: File,
      dataFileLength: Long,
      offset: Long): TestBlockManager = {
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
      dataFileLength,
      offset)
  }

  def initializeServers(dataFile: File, dataFileLength: Long, readOffset: Long = 0): Unit = {
    MockitoAnnotations.initMocks(this)
    when(blockManagerMaster.registerBlockManager(
      any[BlockManagerId], any[Long], any[Long], any[RpcEndpointRef])).thenReturn(null)
    when(rpcEnv.setupEndpoint(any[String], any[RpcEndpoint])).thenReturn(rpcEndpointRef)
    blockManager = getTestBlockManager(localPort, dataFile, dataFileLength, readOffset)
    blockManager.initialize(defaultConf.getAppId)
    externalBlockManager = getTestBlockManager(remotePort, dataFile, dataFileLength, readOffset)
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
      shuffleId = 0,
      numMaps = NUM_MAPS,
      dependency = dependency)

    // We cannot mock the TaskContext because it taskMetrics() gets called at every next()
    // call on the reader, and Mockito will try to log all calls to taskMetrics(), thus OOM-ing
    // the test
    class TestTaskContext extends TaskContext {
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

    val taskContext = new TestTaskContext
    TaskContext.setTaskContext(taskContext)

    var dataBlockId: BlockManagerId = execBlockManagerId
    if (!fetchLocal) {
      dataBlockId = remoteBlockManagerId
    }

    when(mapOutputTracker.getMapSizesByExecutorId(0, 0, 1))
      .thenReturn {
        val shuffleBlockIdsAndSizes = (0 until NUM_MAPS).map { mapId =>
          val shuffleBlockId = ShuffleBlockId(0, mapId, 0)
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

  def generateDataOnDisk(size: Int, file: File, recordOffset: Int): (Long, Long) = {
    // scalastyle:off println
    println("Generating test data with num records: " + size)
    class ManualCloseFileOutputStream(file: File) extends FileOutputStream(file, true) {
      override def close(): Unit = {
        flush()
      }

      def manualClose(): Unit = {
        flush()
        super.close()
      }
    }

    val dataOutput = new ManualCloseFileOutputStream(file)
    val random = new Random(123)
    val serializerInstance = serializer.newInstance()

    var countingOutput = new CountingOutputStream(dataOutput)
    var serializedOutput = serializerInstance.serializeStream(countingOutput)
    var readOffset = 0L
    try {
      (1 to size).foreach { i => {
        if (i % 1000000 == 0) {
          println("Wrote " + i + " test data points")
        }
        if (i == recordOffset) {
          serializedOutput.close()
          readOffset = countingOutput.getCount
          countingOutput = new CountingOutputStream(dataOutput)
          serializedOutput = serializerInstance.serializeStream(countingOutput)
        }
        val x = random.alphanumeric.take(DEFAULT_DATA_STRING_SIZE).mkString
        serializedOutput.writeKey(x)
        serializedOutput.writeValue(x)
      }}
    } finally {
      serializedOutput.close()
      dataOutput.manualClose()
    }
    (countingOutput.getCount, readOffset)
    // scalastyle:off println
  }

  class TestDataFile(file: File, length: Long, offset: Long) {
    def getFile(): File = file
    def getLength(): Long = length
    def getOffset(): Long = offset
  }

  def runWithTestDataFile(size: Int, readOffset: Int = 0)(func: TestDataFile => Unit): Unit = {
    val tempDataFile = File.createTempFile("test-data", "", tempDir)
    val dataFileLengthAndOffset = generateDataOnDisk(size, tempDataFile, readOffset)
    initializeServers(tempDataFile, dataFileLengthAndOffset._1, dataFileLengthAndOffset._2)
    func(new TestDataFile(tempDataFile, dataFileLengthAndOffset._1, dataFileLengthAndOffset._2))
    tempDataFile.delete()
    stopServers()
  }

  def addBenchmarkCase(
      benchmark: Benchmark,
      name: String,
      func: () => BlockStoreShuffleReader[String, String],
      assertSize: Option[Int] = Option.empty): Unit = {
    benchmark.addTimerCase(name) { timer =>
      val reader = func()
      timer.startTiming()
      val numRead = reader.read().length
      timer.stopTiming()
      if (assertSize.isDefined) {
        assert(numRead == assertSize.get)
      }
    }
  }

  def runLargeDatasetTests(): Unit = {
    runWithTestDataFile(TEST_DATA_SIZE) { testDataFile =>
      val baseBenchmark =
        new Benchmark("no aggregation or sorting",
          TEST_DATA_SIZE,
          minNumIters = MIN_NUM_ITERS,
          output = output,
          outputPerIteration = true)
      addBenchmarkCase(
        baseBenchmark,
        "local fetch",
        () => setupReader(testDataFile.getFile(), testDataFile.getLength(), fetchLocal = true),
        assertSize = Option.apply(TEST_DATA_SIZE * NUM_MAPS))
      addBenchmarkCase(
        baseBenchmark,
        "remote rpc fetch",
        () => setupReader(testDataFile.getFile(), testDataFile.getLength(), fetchLocal = false),
        assertSize = Option.apply(TEST_DATA_SIZE * NUM_MAPS))
      baseBenchmark.run()
    }
  }

  def runSmallDatasetTests(): Unit = {
    runWithTestDataFile(SMALLER_DATA_SIZE) { testDataFile =>
      def createCombiner(i: String): String = i
      def mergeValue(i: String, j: String): String = if (Ordering.String.compare(i, j) > 0) i else j
      def mergeCombiners(i: String, j: String): String =
        if (Ordering.String.compare(i, j) > 0) i else j
      val aggregator =
        new Aggregator[String, String, String](createCombiner, mergeValue, mergeCombiners)
      val aggregationBenchmark =
        new Benchmark("with aggregation",
          SMALLER_DATA_SIZE,
          minNumIters = MIN_NUM_ITERS,
          output = output,
          outputPerIteration = true)
      addBenchmarkCase(
        aggregationBenchmark,
        "local fetch",
        () => setupReader(
          testDataFile.getFile(),
          testDataFile.getLength(),
          fetchLocal = true,
          aggregator = Some(aggregator)))
      addBenchmarkCase(
        aggregationBenchmark,
        "remote rpc fetch",
        () => setupReader(
          testDataFile.getFile(),
          testDataFile.getLength(),
          fetchLocal = false,
          aggregator = Some(aggregator)))
      aggregationBenchmark.run()


      val sortingBenchmark =
        new Benchmark("with sorting",
          SMALLER_DATA_SIZE,
          minNumIters = MIN_NUM_ITERS,
          output = output,
          outputPerIteration = true)
      addBenchmarkCase(
        sortingBenchmark,
        "local fetch",
        () => setupReader(
          testDataFile.getFile(),
          testDataFile.getLength(),
          fetchLocal = true,
          sorter = Some(Ordering.String)),
        assertSize = Option.apply(SMALLER_DATA_SIZE * NUM_MAPS))
      addBenchmarkCase(
        sortingBenchmark,
        "remote rpc fetch",
        () => setupReader(
          testDataFile.getFile(),
          testDataFile.getLength(),
          fetchLocal = false,
          sorter = Some(Ordering.String)),
        assertSize = Option.apply(SMALLER_DATA_SIZE * NUM_MAPS))
      sortingBenchmark.run()
    }
  }

  def runSeekTests(): Unit = {
    runWithTestDataFile(SMALLER_DATA_SIZE, readOffset = SMALLER_DATA_SIZE) { testDataFile =>
      val seekBenchmark =
        new Benchmark("with seek",
          SMALLER_DATA_SIZE,
          minNumIters = MIN_NUM_ITERS,
          output = output)

      addBenchmarkCase(
        seekBenchmark,
        "seek to last record",
        () => setupReader(testDataFile.getFile(), testDataFile.getLength(), fetchLocal = false),
        Option.apply(NUM_MAPS))
      seekBenchmark.run()
    }
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    tempDir = Utils.createTempDir(null, "shuffle")

    runBenchmark("BlockStoreShuffleReader reader") {
      runLargeDatasetTests()
      runSmallDatasetTests()
      runSeekTests()
    }

    FileUtils.deleteDirectory(tempDir)
  }
}
