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
package org.apache.spark.util.collection

import java.io._

import com.esotericsoftware.kryo.io.{Output, Input}
import com.esotericsoftware.kryo.{Kryo, Serializer => KSerializer}
import com.google.common.io.ByteStreams
import org.apache.spark.SparkEnv
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.serializer.{DeserializationStream, Serializer}
import org.apache.spark.storage.{BlockId, BlockManager}

import scala.collection.generic.Growable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/**
 * List that can spill some of its contents to disk if its contents cannot be held in memory.
 * Implementation is based heavily on `org.apache.spark.util.collection.ExternalAppendOnlyMap}`
 */
@SerialVersionUID(1L)
private[spark] class ExternalList[T: ClassTag]
    extends Growable[T]
    with Iterable[T]
    with Spillable[SizeTrackingCompactBuffer[T]]
    with Serializable {

  private val sparkConf = SparkEnv.get.conf
  private val blockManager: BlockManager = SparkEnv.get.blockManager
  private val diskBlockManager = blockManager.diskBlockManager
  private val fileBufferSize =
  // Use getSizeAsKb (not bytes) to maintain backwards compatibility if no units are provided
    sparkConf.getSizeAsKb("spark.shuffle.file.buffer", "32k").toInt * 1024
  private val serializerBatchSize = sparkConf.getLong("spark.shuffle.spill.batchSize", 10000)
  private val serializer: Serializer = SparkEnv.get.serializer
  private val ser = serializer.newInstance()
  private val spilledLists = new ArrayBuffer[Iterable[T]]

  private var curWriteMetrics: ShuffleWriteMetrics = _
  // Number of bytes spilled in total
  private var _diskBytesSpilled = 0L
  // Write metrics for current spill
  private var list = new SizeTrackingCompactBuffer[T]()
  private var numItems = 0

  override def size() = numItems

  override def +=(value: T) = {
    list += value
    if (maybeSpill(list, list.estimateSize())) {
      list = new SizeTrackingCompactBuffer
    }
    numItems += 1
    this
  }

  override def clear(): Unit = {
    spilledLists.clear()
    list = new SizeTrackingCompactBuffer[T]()
  }

  /**
   * Spills the current in-memory collection to disk, and releases the memory.
   * Logic is very similar to `ExternalAppendOnlyMap` - with the difference that
   * we must hold iterables, not iterators, as this lists' iterator may be requested
   * multiple times.
   *
   * @param collection collection to spill to disk
   */
  override protected def spill(collection: SizeTrackingCompactBuffer[T]): Unit = {
    val (blockId, file) = diskBlockManager.createTempLocalBlock()
    curWriteMetrics = new ShuffleWriteMetrics()
    var writer = blockManager.getDiskWriter(blockId, file, ser, fileBufferSize, curWriteMetrics)
    var objectsWritten = 0

    // List of batch sizes (bytes) in the order they are written to disk
    val batchSizes = new ArrayBuffer[Long]

    // Flush the disk writer's contents to disk, and update relevant variables
    def flush(): Unit = {
      val w = writer
      writer = null
      w.commitAndClose()
      _diskBytesSpilled += curWriteMetrics.shuffleBytesWritten
      batchSizes.append(curWriteMetrics.shuffleBytesWritten)
      objectsWritten = 0
    }

    var success = false
    try {
      val it = list.iterator
      while (it.hasNext) {
        val kv = it.next()
        writer.write(0, kv)
        objectsWritten += 1

        if (objectsWritten == serializerBatchSize) {
          flush()
          curWriteMetrics = new ShuffleWriteMetrics()
          writer = blockManager.getDiskWriter(blockId, file, ser, fileBufferSize, curWriteMetrics)
        }
      }
      if (objectsWritten > 0) {
        flush()
      } else if (writer != null) {
        val w = writer
        writer = null
        w.revertPartialWritesAndClose()
      }
      success = true
    } finally {
      if (!success) {
        // This code path only happens if an exception was thrown above before we set success;
        // close our stuff and let the exception be thrown further
        if (writer != null) {
          writer.revertPartialWritesAndClose()
        }
        if (file.exists()) {
          file.delete()
        }
      }
    }

    spilledLists += new DiskListIterable(file, blockId, batchSizes)
  }

  override def iterator: Iterator[T] = {
    val myIt = list.iterator
    val allIts = spilledLists.map(_.iterator) ++ Seq(myIt)
    allIts.foldLeft(Iterator[T]())(_ ++ _)
  }

  private class DiskListIterable(file: File, blockId: BlockId, batchSizes: ArrayBuffer[Long])
      extends Iterable[T] {
    override def iterator: Iterator[T] = {
      new DiskListIterator(file, blockId, batchSizes)
    }
  }

  private class DiskListIterator(file: File, blockId: BlockId, batchSizes: ArrayBuffer[Long])
      extends Iterator[T] {
    private val batchOffsets = batchSizes.scanLeft(0L)(_ + _)  // Size will be batchSize.length + 1
    assert(file.length() == batchOffsets.last,
      "File length is not equal to the last batch offset:\n" +
        s"    file length = ${file.length}\n" +
        s"    last batch offset = ${batchOffsets.last}\n" +
        s"    all batch offsets = ${batchOffsets.mkString(",")}"
    )

    private var batchIndex = 0  // Which batch we're in
    private var fileStream: FileInputStream = null

    // An intermediate stream that reads from exactly one batch
    // This guards against pre-fetching and other arbitrary behavior of higher level streams
    private var deserializeStream = nextBatchStream()
    private var nextItem: Option[T] = None
    private var objectsRead = 0

    /**
     * Construct a stream that reads only from the next batch.
     */
    private def nextBatchStream(): DeserializationStream = {
      // Note that batchOffsets.length = numBatches + 1 since we did a scan above; check whether
      // we're still in a valid batch.
      if (batchIndex < batchOffsets.length - 1) {
        if (deserializeStream != null) {
          deserializeStream.close()
          fileStream.close()
          deserializeStream = null
          fileStream = null
        }

        val start = batchOffsets(batchIndex)
        fileStream = new FileInputStream(file)
        fileStream.getChannel.position(start)
        batchIndex += 1

        val end = batchOffsets(batchIndex)

        assert(end >= start, "start = " + start + ", end = " + end +
          ", batchOffsets = " + batchOffsets.mkString("[", ", ", "]"))

        val bufferedStream = new BufferedInputStream(ByteStreams.limit(fileStream, end - start))
        val compressedStream = blockManager.wrapForCompression(blockId, bufferedStream)
        ser.deserializeStream(compressedStream)
      } else {
        // No more batches left
        cleanup()
        null
      }
    }

    private def readNextItem(): Option[T] = {
      try {
        // Ignore the key because we only wrote 0S
        deserializeStream.readKey()
        val t = deserializeStream.readValue()
        objectsRead += 1
        if (objectsRead == serializerBatchSize) {
          objectsRead = 0
          deserializeStream = nextBatchStream()
        }
        Some(t)
      } catch {
        case e: EOFException =>
          cleanup()
          None
      }
    }

    override def hasNext: Boolean = {
      if (!nextItem.isDefined) {
        if (deserializeStream == null) {
          return false
        }
        nextItem = readNextItem()
      }
      nextItem.isDefined
    }

    override def next(): T = {
      val item = nextItem match {
        case None => readNextItem()
        case Some(theItem) => nextItem
      }
      if (!item.isDefined) {
        throw new NoSuchElementException
      }
      nextItem = None
      item match {
        case Some(value) => value
        case None => null.asInstanceOf[T]
      }
    }

    private def cleanup() {
      batchIndex = batchOffsets.length  // Prevent reading any other batch
      val ds = deserializeStream
      deserializeStream = null
      fileStream = null
      ds.close()
      file.delete()
    }
  }

  @throws(classOf[IOException])
  private def writeObject(stream: ObjectOutputStream): Unit = {
    stream.writeInt(this.size)
    val it = this.iterator
    while (it.hasNext) {
      stream.writeObject(it.next)
    }
  }

  @throws(classOf[IOException])
  private def readObject(stream: ObjectInputStream): Unit = {
    val listSize = stream.readInt()
    list = new SizeTrackingCompactBuffer[T]
    for(i <- 0L until listSize) {
      val newItem = stream.readObject().asInstanceOf[T]
      require(newItem != null)
      this.+=(newItem)
    }
  }
}

private[spark] object ExternalList {
  def apply[T: ClassTag](): ExternalList[T] = new ExternalList[T]

  def apply[T: ClassTag](value: T): ExternalList[T] = {
    val buf = new ExternalList[T]
    buf += value
    buf
  }
}

private[spark] class ExternalListSerializer[T: ClassTag] extends KSerializer[ExternalList[T]] {
  override def write(kryo: Kryo, output: Output, list: ExternalList[T]): Unit = {
    output.writeInt(list.size)
    val it = list.iterator
    while (it.hasNext) {
      kryo.writeClassAndObject(output, it.next())
    }
  }

  override def read(kryo: Kryo, input: Input, clazz: Class[ExternalList[T]]): ExternalList[T] = {
    val listToRead = new ExternalList[T]
    val listSize = input.readInt()
    for (i <- 0L until listSize) {
      val newItem = kryo.readClassAndObject(input).asInstanceOf[T]
      listToRead += newItem
    }
    listToRead
  }
}
