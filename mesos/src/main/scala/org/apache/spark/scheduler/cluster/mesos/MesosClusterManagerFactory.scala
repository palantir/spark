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

import org.apache.spark.SparkContext
import org.apache.spark.clustermanager.plugins.scheduler.ClusterManagerExecutorLifecycleHandler
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.scheduler.{ExternalClusterManager, ExternalClusterManagerFactory, SchedulerBackend, TaskScheduler, TaskSchedulerImpl}
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend

private[spark] class MesosClusterManagerFactory extends ExternalClusterManagerFactory {
  override def canCreate(masterURL: String): Boolean = {
    masterURL.startsWith("mesos")
  }

  override def newExternalClusterManager(
      sc: SparkContext, masterURL: String): ExternalClusterManager = {
    require(!sc.conf.get(IO_ENCRYPTION_ENABLED),
      "I/O encryption is currently not supported in Mesos.")
    val scheduler = new TaskSchedulerImpl(sc)
    val mesosSchedulerErrorHandler = new MesosSchedulerErrorHandler with Logging {
      override def handleError(message: String): Unit = {
        logError(s"Mesos error: $message")
        scheduler.error(message)
      }
    }
    val mesosUrl = MesosClusterManagerFactory.MESOS_REGEX.findFirstMatchIn(masterURL).get.group(1)
    val coarse = sc.conf.getBoolean("spark.mesos.coarse", defaultValue = true)
    val (schedulerBackend, executorProvider) = if (coarse) {
      val executorProvider = new MesosExecutorProvider(
        sc.conf,
        mesosSchedulerErrorHandler,
        sc.env.rpcEnv,
        sc,
        sc.env.securityManager,
        mesosUrl)
      val coarseGrainedBackend = new CoarseGrainedSchedulerBackend(
        scheduler,
        executorProvider,
        ClusterManagerExecutorLifecycleHandler.DEFAULT,
        sc.env.rpcEnv,
        sc)
      executorProvider.initialize(coarseGrainedBackend)
      (Some(coarseGrainedBackend), Some(executorProvider))
    } else {
      (Some(new MesosFineGrainedSchedulerBackend(
        scheduler,
        sc,
        mesosUrl)), Option.empty[MesosExecutorProvider])
    }
    ExternalClusterManager(
      maybeCustomSchedulerBackend = schedulerBackend,
      maybeExecutorProvider = executorProvider,
      maybeDriverLogUrlsProvider = None)
  }

  override def initializeScheduler(scheduler: TaskScheduler, backend: SchedulerBackend): Unit = {
    scheduler.initializeBackend(backend)
  }
}

private[mesos] object MesosClusterManagerFactory {
  val MESOS_REGEX = """mesos://(.*)""".r
}

