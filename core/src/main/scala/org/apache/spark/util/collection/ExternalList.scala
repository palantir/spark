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

import scala.reflect.ClassTag
import scala.collection.generic.Growable
import scala.collection.mutable.ArrayBuffer

import com.esotericsoftware.kryo.io.{Output, Input}
import com.esotericsoftware.kryo.{Kryo, Serializer => KSerializer}

import org.apache.spark.serializer.DeserializationStream
import org.apache.spark.storage.{DiskBlockObjectWriter, BlockId}


/**
 * List that can spill some of its contents to disk if its contents cannot be held in memory.
 * Implementation is based heavily on `org.apache.spark.util.collection.ExternalAppendOnlyMap}`
 */
@SerialVersionUID(1L)
private[spark] class ExternalList[T](implicit private var tag: ClassTag[T])
    extends Growable[T]
    with Iterable[T]
    with SpillableCollection[T, SizeTrackingCompactBuffer[T]]
    with Serializable {

  // Lazy vals so that this isn't created multiple times but still can be re-instantiated properly
  // after serialization
  private lazy val spilledLists = new ArrayBuffer[Iterable[T]]

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
      extends DiskIterator(file, blockId, batchSizes) {
    override protected def readNextItemFromStream(deserializeStream: DeserializationStream): T = {
      deserializeStream.readKey[Int]()
      deserializeStream.readValue[T]()
    }

    // Need to be able to iterate multiple times, so don't clean up the file every time
    override protected def shouldCleanupFileAfterOneIteration(): Boolean = false
  }

  @throws(classOf[IOException])
  private def writeObject(stream: ObjectOutputStream): Unit = {
    stream.writeObject(tag)
    stream.writeInt(this.size)
    val it = this.iterator
    while (it.hasNext) {
      stream.writeObject(it.next)
    }
  }

  @throws(classOf[IOException])
  private def readObject(stream: ObjectInputStream): Unit = {
    tag = stream.readObject().asInstanceOf[ClassTag[T]]
    val listSize = stream.readInt()
    list = new SizeTrackingCompactBuffer[T]
    for(i <- 0L until listSize) {
      val newItem = stream.readObject().asInstanceOf[T]
      this.+=(newItem)
    }
  }

  override protected def getIteratorForCurrentSpillable(): Iterator[T] = list.iterator
  override protected def recordNextSpilledPart(file: File, blockId: BlockId, batchSizes: ArrayBuffer[Long]): Unit = {
    spilledLists += new DiskListIterable(file, blockId, batchSizes)
  }
  override protected def writeNextObject(c: T, writer: DiskBlockObjectWriter): Unit = {
    writer.write(0, c)
  }
}

/**
 * Companion object for constants and singleton-references that we don't want to lose when
 * Java-serializing
 */
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
