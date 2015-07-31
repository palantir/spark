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

import java.io.File
import java.lang.ref.WeakReference

import scala.language.existentials
import scala.reflect.ClassTag

import org.apache.spark.{SparkEnv, SparkContext, SparkConf, SparkFunSuite}
import org.apache.spark.util.collection.ExternalListSuite._
import org.apache.spark.serializer.{KryoSerializer, JavaSerializer, SerializerInstance}

import org.junit.Assert.{assertEquals, assertTrue, assertFalse}
import org.scalatest.concurrent.Eventually._
import org.scalatest.time.SpanSugar._

class ExternalListSuite extends SparkFunSuite with Serializable {

  val conf = new SparkConf(false)
  conf.set("spark.kryoserializer.buffer.max", "2046m")
  conf.set("spark.shuffle.spill.initialMemoryThreshold", "1")
  conf.set("spark.shuffle.spill.batchSize", "10")
  conf.set("spark.shuffle.memoryFraction", "0.035")
  conf.set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
  conf.set("spark.task.maxFailures", "1")
  conf.setMaster("local[8]")
  conf.setAppName("test")

  val sparkContext = new SparkContext(conf)

  test("Serializing and deserializing a spilled list should produce the same values") {
    var serializer = new KryoSerializer(conf).newInstance()
    var list = new ExternalList[Int]
    // Test big list for Kryo because it's fast enough to handle it
    // and we want to test the case where the list would spill to disk
    for (i <- 0 to 5000000) {
      list += i
    }
    testSerialization(serializer, list)
    serializer = new JavaSerializer(conf).newInstance()
    list = new ExternalList[Int]
    // Test smaller list for Java serialization since serializing with Java is
    // really slow, and we already test serialization causing spilling in the Kryo case
    for (i <- 0 to 1000000) {
      list += i
    }
    testSerialization(serializer, list)
  }

  val totalRddSize = 7200000
  val numBuckets = 5
  val rawLargeRdd = sparkContext.parallelize(1 to totalRddSize)
  test("Lists that are cached should be accessible twice, but when unpersisted are cleaned up.") {
    val groupedRdd = rawLargeRdd.map(x => (x % numBuckets, x)).groupByKey
    val cachedRdd = groupedRdd.cache()
    cachedRdd.foreach(validateList(totalRddSize, numBuckets, _))
    runGC()
    // GC on the Cached RDD shouldn't trigger the cleanup
    cachedRdd.foreach(validateList(totalRddSize, numBuckets, _))
    val filePaths = cachedRdd.map(_._2.asInstanceOf[ExternalList[Int]].getBackingFileLocations()).collect
    filePaths.foreach(paths => {
      paths.foreach(f => assertTrue(new File(f).exists()))
    })
    cachedRdd.unpersist(true)
    runGC()
    checkFilesEventuallyRemoved(filePaths)
    cachedRdd.foreach(validateList(totalRddSize, numBuckets, _))
  }

  test("List that is created in a task and released immediately should eventually clean up") {
    val filePaths = rawLargeRdd
      .map(x => (x % numBuckets, x))
      .groupByKey
      .map(x => x._2.asInstanceOf[ExternalList[Int]].getBackingFileLocations()).collect
    runGC()
    checkFilesEventuallyRemoved(filePaths)
  }

  private def checkFilesEventuallyRemoved(filePaths: Array[Iterable[String]]) {
    eventually(timeout(15000 millis), interval(100 millis)) {
      filePaths.foreach(paths => {
        paths.foreach(f => assertFalse(new File(f).exists()))
      })
    }
  }

  /** Run GC and make sure it actually has run */
  private def runGC() {
    val weakRef = new WeakReference(new Object())
    val startTime = System.currentTimeMillis
    System.gc() // Make a best effort to run the garbage collection. It *usually* runs GC.
    // Wait until a weak reference object has been GCed
    while (System.currentTimeMillis - startTime < 10000 && weakRef.get != null) {
      System.gc()
      Thread.sleep(200)
    }
  }

  private def testSerialization[T: ClassTag](
      serializer: SerializerInstance,
      list: ExternalList[T]): Unit = {
    val bytes = serializer.serialize(list)
    var readList = serializer.deserialize(bytes).asInstanceOf[ExternalList[Int]]
    val originalIt = list.iterator
    var readIt = readList.iterator
    while (originalIt.hasNext) {
      assert (originalIt.next == readIt.next)
    }
    assert (!readIt.hasNext)
    val filePaths = readList.getBackingFileLocations()
    readList = null
    readIt = null
    runGC()
    eventually(timeout(15000 millis), interval(100 millis)) {
      filePaths.foreach(path => assertFalse(new File(path).exists()))
    }
  }

}

object ExternalListSuite {
  def validateList(totalRddSize: Int, numBuckets: Int, kv: (Int, Iterable[Int])): Unit = {
    var numItems = 0
    for (valsInBucket <- kv._2) {
      numItems += 1
      // Can't use scala assertions because including assert statements makes closures not serializable.
      assertEquals(s"Value $valsInBucket should not be in bucket ${kv._1}", kv._1, valsInBucket % numBuckets)
    }
    assertEquals(s"Number of items in bucket ${kv._1} is incorrect.", totalRddSize / numBuckets, numItems)
  }
}
