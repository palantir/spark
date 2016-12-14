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

import org.apache.spark.SparkContext
import org.apache.spark.clustermanager.plugins.driverlogs.ClusterManagerDriverLogUrlsProvider
import org.apache.spark.clustermanager.plugins.scheduler.{ClusterManagerExecutorProvider, ClusterManagerExecutorProviderFactory}

/**
 * A cluster manager interface to plugin external scheduler.
 */
private[spark] trait ExternalClusterManager {

  /**
   * Check if this cluster manager instance can create scheduler components
   * for a certain master URL.
   * @param masterURL the master URL
   * @return True if the cluster manager can create scheduler backend/
   */
  def canCreate(masterURL: String): Boolean

  /**
   * Create a task scheduler instance for the given SparkContext
   * @param sc SparkContext
   * @param masterURL the master URL
   * @return TaskScheduler that will be responsible for task handling
   */
  def createTaskScheduler(sc: SparkContext, masterURL: String): TaskScheduler

  /**
   * Create a scheduler backend for the given SparkContext and scheduler. This is
   * called after task scheduler is created using [[ExternalClusterManager.createTaskScheduler()]].
   * Return None if the default scheduler backend implementation is to be used; note that the
   * default scheduler backend will still be customized via the
   * {@link org.apache.spark.scheduler.ClusterManagerExecutorProvider}.
   * @param sc SparkContext
   * @param masterURL the master URL
   * @param scheduler TaskScheduler that will be used with the scheduler backend.
   * @return SchedulerBackend that works with a TaskScheduler
   */
  def createCustomSchedulerBackend(sc: SparkContext,
      masterURL: String,
      scheduler: TaskScheduler): Option[SchedulerBackend] = None

  def createExecutorProviderFactory(masterURL: String, deployMode: String)
      : ClusterManagerExecutorProviderFactory[_ <: ClusterManagerExecutorProvider]

  def createDriverLogUrlsProvider(sc: SparkContext): Option[ClusterManagerDriverLogUrlsProvider]
    = None

  /**
   * Initialize task scheduler and backend scheduler. This is called after the
   * scheduler components are created
   * @param scheduler TaskScheduler that will be responsible for task handling
   * @param backend SchedulerBackend that works with a TaskScheduler
   */
  def initialize(scheduler: TaskScheduler, backend: SchedulerBackend): Unit
}
