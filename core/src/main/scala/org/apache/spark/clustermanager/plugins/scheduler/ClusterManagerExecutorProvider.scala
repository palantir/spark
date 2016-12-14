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
package org.apache.spark.clustermanager.plugins.scheduler

import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.Future

/**
 * Responsible for creating and destroying executors by interacting with a cluster manager.
 * Primarily invoked by the {@link SchedulerBackend}
 */
trait ClusterManagerExecutorProvider {
  private val appId = "spark-application-" + System.currentTimeMillis

  def start(): Unit = {}

  def stop(): Unit = {}

  def reset(): Unit = {}

  /**
   * Request executors from the cluster manager by specifying the total number desired,
   * including existing pending and running executors.
   *
   * The semantics here guarantee that we do not over-allocate executors for this application,
   * since a later request overrides the value of any prior request. The alternative interface
   * of requesting a delta of executors risks double counting new executors when there are
   * insufficient resources to satisfy the first request. We make the assumption here that the
   * cluster manager will eventually fulfill all requests when resources free up.
   *
   * @return a future whose evaluation indicates whether the request is acknowledged.
   */
  def doRequestTotalExecutors(
      requestedTotal: Int,
      localityAwareTasks: Int,
      hostToLocalTaskCount: Map[String, Int]): Future[Boolean]

  /**
   * Kill the given list of executors through the cluster manager.
   *
   * @return whether the kill request is acknowledged.
   */
  def doKillExecutors(executorIds: Seq[String]): Future[Boolean]

  def minRegisteredRatio: Double

  def sufficientResourcesRegistered(
      totalCoreCount: AtomicInteger,
      totalRegisteredExecutors: AtomicInteger): Boolean = true

  /**
   * Get an application ID associated with the job.
   *
   * @return An application ID
   */
  def applicationId(): String = appId

  def applicationAttemptId(): Option[String] = None
}
