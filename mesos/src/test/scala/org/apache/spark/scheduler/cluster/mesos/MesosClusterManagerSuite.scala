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

package org.apache.spark.scheduler.cluster.mesos

import org.apache.spark._
import org.apache.spark.internal.config._

class MesosClusterManagerSuite extends SparkFunSuite with LocalSparkContext {
  def testURL(masterURL: String, expectedSchedulerClass: Option[Class[_]] = None,
              expectedExecutorProviderFactoryClass: Option[Class[_]] = None,
              coarse: Boolean) {
    require(expectedSchedulerClass.isDefined || expectedExecutorProviderFactoryClass.isDefined)
    val conf = new SparkConf().set("spark.mesos.coarse", coarse.toString)
    sc = new SparkContext("local", "test", conf)
    val clusterManager = new MesosClusterManager()

    assert(clusterManager.canCreate(masterURL))
    val taskScheduler = clusterManager.createTaskScheduler(sc, masterURL)
    val sched = clusterManager.createCustomSchedulerBackend(sc, masterURL, taskScheduler)
    sched match {
      case Some(impl) => assert(impl.getClass === expectedSchedulerClass.get)
      case None =>
        require(expectedExecutorProviderFactoryClass.isDefined)
        val clusterMode = clusterManager.createExecutorProviderFactory(masterURL, "cluster")
        assert(clusterMode.getClass === expectedExecutorProviderFactoryClass.get)
        val clientMode = clusterManager.createExecutorProviderFactory(masterURL, "client")
        assert(clientMode.getClass === expectedExecutorProviderFactoryClass.get)
    }
  }

  test("mesos fine-grained") {
    testURL("mesos://localhost:1234",
      expectedSchedulerClass = Some(classOf[MesosFineGrainedSchedulerBackend]),
      coarse = false)
  }

  test("mesos coarse-grained") {
    testURL("mesos://localhost:1234",
      expectedExecutorProviderFactoryClass = Some(classOf[MesosExecutorProviderFactory]),
      coarse = true)
  }

  test("mesos with zookeeper") {
    testURL("mesos://zk://localhost:1234,localhost:2345",
        expectedSchedulerClass = Some(classOf[MesosFineGrainedSchedulerBackend]),
        coarse = false)
  }

  test("mesos with i/o encryption throws error") {
    val se = intercept[SparkException] {
      val conf = new SparkConf().setAppName("test").set(IO_ENCRYPTION_ENABLED, true)
      sc = new SparkContext("mesos", "test", conf)
    }
    assert(se.getCause().isInstanceOf[IllegalArgumentException])
  }
}
