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
import org.apache.spark.internal.config._
import org.apache.spark.scheduler.{ExternalClusterManager, ExternalClusterManagerFactory, SchedulerBackend, TaskScheduler, TaskSchedulerImpl}

private[spark] class MesosClusterManagerFactory extends ExternalClusterManagerFactory {
  import MesosClusterManager._

  override def canCreate(masterURL: String): Boolean = {
    masterURL.startsWith("mesos")
  }

  override def newExternalClusterManager(
      sc: SparkContext, masterURL: String): ExternalClusterManager = {
    val scheduler = new TaskSchedulerImpl(sc)
    val maybeSchedulerBackend = createCustomSchedulerBackend(sc, masterURL, scheduler)
    val maybeExecutorProviderFactory = maybeSchedulerBackend match {
      case Some(_) => Option.empty[MesosExecutorProviderFactory]
      case None => Some(new MesosExecutorProviderFactory(masterURL))
    }
    ExternalClusterManager(
      maybeCustomSchedulerBackend = maybeSchedulerBackend,
      maybeExecutorProviderFactory = maybeExecutorProviderFactory,
      maybeDriverLogUrlsProvider = None)
  }

  private def createCustomSchedulerBackend(sc: SparkContext,
      masterURL: String,
      scheduler: TaskSchedulerImpl): Option[SchedulerBackend] = {
    require(!sc.conf.get(IO_ENCRYPTION_ENABLED),
      "I/O encryption is currently not supported in Mesos.")

    val mesosUrl = MESOS_REGEX.findFirstMatchIn(masterURL).get.group(1)
    val coarse = sc.conf.getBoolean("spark.mesos.coarse", defaultValue = true)
    if (coarse) {
      None
    } else {
      Some(new MesosFineGrainedSchedulerBackend(
        scheduler,
        sc,
        mesosUrl))
    }
  }

  override def initializeScheduler(scheduler: TaskScheduler, backend: SchedulerBackend): Unit = {
    scheduler.initializeBackend(backend)
  }
}

private[mesos] object MesosClusterManager {
  val MESOS_REGEX = """mesos://(.*)""".r
}

