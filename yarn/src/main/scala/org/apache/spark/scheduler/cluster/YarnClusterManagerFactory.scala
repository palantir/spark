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
package org.apache.spark.scheduler.cluster

import org.apache.spark.{SparkContext, SparkException}
import org.apache.spark.clustermanager.plugins.driverlogs.ClusterManagerDriverLogUrlsProvider
import org.apache.spark.deploy.yarn.YarnClusterDriverLogUrlsProvider
import org.apache.spark.scheduler.{ExternalClusterManager, ExternalClusterManagerFactory, SchedulerBackend, TaskScheduler, TaskSchedulerImpl}

class YarnClusterManagerFactory extends ExternalClusterManagerFactory {
  override def canCreate(masterURL: String): Boolean = {
    masterURL == "yarn"
  }

  override def newExternalClusterManager(
      sc: SparkContext,
      masterURL: String): ExternalClusterManager = {
    val amRegistrationEndpoint = new AMRegistrationEndpoint(sc.env.rpcEnv)
    sc.env.rpcEnv.setupEndpoint(AMRegistrationEndpoint.ENDPOINT_NAME, amRegistrationEndpoint)
    val executorProviderFactory = new YarnExecutorProviderFactory(amRegistrationEndpoint,
      sc.deployMode)
    val executorLifecycleHandler = new YarnExecutorLifecycleHandler(amRegistrationEndpoint, sc.conf)
    val (taskScheduler, driverLogUrlsProvider) = sc.deployMode.toLowerCase match {
      case "cluster" =>
        (new YarnClusterScheduler(sc), Some(new YarnClusterDriverLogUrlsProvider(sc)))
      case "client" => (new YarnScheduler(sc), Option.empty[ClusterManagerDriverLogUrlsProvider])
      case _ => throw new SparkException(s"Unknown deploy mode: ${sc.deployMode}")
    }
    val schedulerBackend = new CoarseGrainedSchedulerBackend(
      taskScheduler.asInstanceOf[TaskSchedulerImpl],
      executorProviderFactory,
      executorLifecycleHandler,
      sc.env.rpcEnv,
      sc)
    ExternalClusterManager(
      maybeCustomExecutorLifecycleHandler = Some(executorLifecycleHandler),
      maybeCustomSchedulerBackend = Some(schedulerBackend),
      maybeExecutorProviderFactory = Some(executorProviderFactory),
      maybeCustomTaskScheduler = Some(taskScheduler),
      maybeDriverLogUrlsProvider = driverLogUrlsProvider)
  }

  override def initializeScheduler(scheduler: TaskScheduler, backend: SchedulerBackend): Unit = {
    scheduler.initializeBackend(backend)
  }

}
