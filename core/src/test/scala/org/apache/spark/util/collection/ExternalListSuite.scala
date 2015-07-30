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

import org.apache.spark.serializer.{KryoSerializer, JavaSerializer, SerializerInstance}
import org.apache.spark.{SparkContext, SparkConf, SparkFunSuite}

import scala.reflect.ClassTag

class ExternalListSuite extends SparkFunSuite {

  val conf = new SparkConf(false)
  conf.set("spark.kryoserializer.buffer.max", "2046m")
  conf.set("spark.shuffle.spill.initialMemoryThreshold", "1")
  conf.set("spark.shuffle.memoryFraction", "0.035")
  conf.setMaster("local[8]")
  conf.setAppName("test")
  val sparkContext = new SparkContext(conf)

  test("Serializing and deserializing a spilled list should produce the same values") {
    var serializer = new KryoSerializer(conf).newInstance()
    var list = new ExternalList[Int]
    // Test big list for Kryo because it's fast enough to handle it
    // and we want to test the case where the list would spill to disk
    for (i <- 0 to 8000000) {
      list += i
    }
    testSerialization(serializer, list)
    serializer = new JavaSerializer(conf).newInstance()
    list = new ExternalList[Int]
    // Test smaller list for Java serialization since serializing with Java is
    // really slow, and we already test serialization causing spilling in the Kryo case
    for (i <- 0 to 1000) {
      list += i
    }
    testSerialization(serializer, list)
  }

  private def testSerialization[T: ClassTag](
      serializer: SerializerInstance,
      list: ExternalList[T]): Unit = {
    val bytes = serializer.serialize(list)
    val readList = serializer.deserialize(bytes).asInstanceOf[ExternalList[Int]]
    val originalIt = list.iterator
    val readIt = readList.iterator
    while (originalIt.hasNext) {
      assert (originalIt.next == readIt.next)
    }
    assert (!readIt.hasNext)
  }

}
