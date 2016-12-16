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

package org.apache.spark.scheduler

import org.apache.spark.clustermanager.plugins.driverlogs.ClusterManagerDriverLogUrlsProvider
import org.apache.spark.clustermanager.plugins.scheduler.{ClusterManagerExecutorLifecycleHandler, ClusterManagerExecutorProvider}

/**
 * A cluster manager interface to plugin external scheduler.
 */
private[spark] case class ExternalClusterManager(
    maybeCustomTaskScheduler: Option[TaskScheduler] = None,
    maybeCustomExecutorLifecycleHandler: Option[ClusterManagerExecutorLifecycleHandler] = None,
    maybeCustomSchedulerBackend: Option[SchedulerBackend] = None,
    maybeExecutorProvider: Option[ClusterManagerExecutorProvider] = None,
    maybeDriverLogUrlsProvider: Option[ClusterManagerDriverLogUrlsProvider] = None) {
  validate()

  private def validate(): Unit = {
    require(maybeCustomSchedulerBackend.isDefined || maybeExecutorProvider.isDefined,
      "Either a custom scheduler backend or an executor provider factory must be set.")
    maybeCustomSchedulerBackend.foreach { _ =>
      require(maybeExecutorProvider.isEmpty, "Should not provide both a custom scheduler" +
        " backend and an executor provider factory.")
    }
    maybeExecutorProvider.foreach { _ =>
      require(maybeCustomSchedulerBackend.isEmpty, "Should not provider both a custom scheduler" +
        " backend and an executor provider factory.")
    }
    require(maybeCustomSchedulerBackend.isDefined || maybeCustomTaskScheduler.isEmpty,
        "When not providing a custom scheduler backend, using a custom task scheduler" +
          " is not supported since the default scheduler backend must use a fixed scheduler" +
          " implementation.")
  }

}
