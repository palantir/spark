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

import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.Future
import scala.util.{Failure, Success}
import scala.util.control.NonFatal

import org.apache.hadoop.yarn.api.records.{ApplicationAttemptId, ApplicationId}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.clustermanager.plugins.scheduler.ClusterManagerExecutorProvider
import org.apache.spark.internal.Logging
import org.apache.spark.rpc._
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages._
import org.apache.spark.ui.JettyUtils
import org.apache.spark.util.{RpcUtils, ThreadUtils}

/**
 * Abstract Yarn scheduler backend that contains common logic
 * between the client and cluster Yarn scheduler backends.
 */
private[spark] abstract class YarnExecutorProvider(
    protected val conf: SparkConf,
    amRegistrationEndpoint: AMRegistrationEndpoint,
    rpcEnv: RpcEnv,
    sc: SparkContext)
  extends ClusterManagerExecutorProvider with Logging {

  override def minRegisteredRatio: Double = math.min(
    1, conf.getDouble("spark.scheduler.minRegisteredResourcesRatio", 0.8))

  protected var totalExpectedExecutors = 0

  private val yarnSchedulerEndpoint = new YarnSchedulerEndpoint(rpcEnv)

  private val yarnSchedulerEndpointRef = rpcEnv.setupEndpoint(
    YarnExecutorProvider.ENDPOINT_NAME, yarnSchedulerEndpoint)

  private implicit val askTimeout = RpcUtils.askRpcTimeout(conf)

  /** Application ID. */
  protected var appId: Option[ApplicationId] = None

  /** Attempt ID. This is unset for client-mode schedulers */
  private var attemptId: Option[ApplicationAttemptId] = None

  /** Scheduler extension services. */
  private val services: SchedulerExtensionServices = new SchedulerExtensionServices()

  /**
   * Bind to YARN. This *must* be done before calling [[start()]].
   * @param appId YARN application ID
   * @param attemptId Optional YARN attempt ID
   */
  protected def bindToYarn(appId: ApplicationId, attemptId: Option[ApplicationAttemptId]): Unit = {
    this.appId = Some(appId)
    this.attemptId = attemptId
  }

  override def start() {
    require(appId.isDefined, "application ID unset")
    val binding = SchedulerExtensionServiceBinding(sc, appId.get, attemptId)
    services.start(binding)
    super.start()
  }

  override def stop(): Unit = {
    try {
      // SPARK-12009: To prevent Yarn allocator from requesting backup for the executors which
      // was Stopped by SchedulerBackend.
      doRequestTotalExecutors(0, 0, Map.empty[String, Int])
      super.stop()
    } finally {
      services.stop()
    }
  }

  /**
   * Get the attempt ID for this run, if the cluster manager supports multiple
   * attempts. Applications run in client mode will not have attempt IDs.
   * This attempt ID only includes attempt counter, like "1", "2".
   * @return The application attempt id, if available.
   */
  override def applicationAttemptId(): Option[String] = {
    attemptId.map(_.getAttemptId.toString)
  }

  /**
   * Get an application ID associated with the job.
   * This returns the string value of [[appId]] if set, otherwise
   * the locally-generated ID from the superclass.
   * @return The application ID
   */
  override def applicationId(): String = {
    appId.map(_.toString).getOrElse {
      logWarning("Application ID is not initialized yet.")
      super.applicationId
    }
  }

  /**
   * Request executors from the ApplicationMaster by specifying the total number desired.
   * This includes executors already pending or running.
   */
  override def doRequestTotalExecutors(
      requestedTotal: Int,
      localityAwareTasks: Int,
      hostToLocalTaskCount: Map[String, Int]): Future[Boolean] = {
    yarnSchedulerEndpointRef.ask[Boolean](
      RequestExecutors(requestedTotal, localityAwareTasks, hostToLocalTaskCount))
  }

  /**
   * Request that the ApplicationMaster kill the specified executors.
   */
  override def doKillExecutors(executorIds: Seq[String]): Future[Boolean] = {
    yarnSchedulerEndpointRef.ask[Boolean](KillExecutors(executorIds))
  }

  override def sufficientResourcesRegistered(
      totalCoreCount: AtomicInteger,
      totalRegisteredExecutors: AtomicInteger): Boolean = {
    totalRegisteredExecutors.get() >= totalExpectedExecutors * minRegisteredRatio
  }

  /**
   * Add filters to the SparkUI.
   */
  private def addWebUIFilter(
      filterName: String,
      filterParams: Map[String, String],
      proxyBase: String): Unit = {
    if (proxyBase != null && proxyBase.nonEmpty) {
      System.setProperty("spark.ui.proxyBase", proxyBase)
    }

    val hasFilter =
      filterName != null && filterName.nonEmpty &&
      filterParams != null && filterParams.nonEmpty
    if (hasFilter) {
      logInfo(s"Add WebUI Filter. $filterName, $filterParams, $proxyBase")
      conf.set("spark.ui.filters", filterName)
      filterParams.foreach { case (k, v) => conf.set(s"spark.$filterName.param.$k", v) }
      sc.ui.foreach { ui => JettyUtils.addFilters(ui.getHandlers, conf) }
    }
  }

  /**
   * An [[RpcEndpoint]] that communicates with the ApplicationMaster.
   */
  private class YarnSchedulerEndpoint(override val rpcEnv: RpcEnv)
    extends ThreadSafeRpcEndpoint with Logging {

    override def receive: PartialFunction[Any, Unit] = {
      case AddWebUIFilter(filterName, filterParams, proxyBase) =>
        addWebUIFilter(filterName, filterParams, proxyBase)
    }

    override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
      case r: RequestExecutors =>
        amRegistrationEndpoint.getCurrentlySetAmEndpoint match {
          case Some(am) =>
            am.ask[Boolean](r).andThen {
              case Success(b) => context.reply(b)
              case Failure(NonFatal(e)) =>
                logError(s"Sending $r to AM was unsuccessful", e)
                context.sendFailure(e)
            }(ThreadUtils.sameThread)
          case None =>
            logWarning("Attempted to request executors before the AM has registered!")
            context.reply(false)
        }

      case k: KillExecutors =>
        amRegistrationEndpoint.getCurrentlySetAmEndpoint match {
          case Some(am) =>
            am.ask[Boolean](k).andThen {
              case Success(b) => context.reply(b)
              case Failure(NonFatal(e)) =>
                logError(s"Sending $k to AM was unsuccessful", e)
                context.sendFailure(e)
            }(ThreadUtils.sameThread)
          case None =>
            logWarning("Attempted to kill executors before the AM has registered!")
            context.reply(false)
        }
    }
  }
}

private[spark] object YarnExecutorProvider {
  val ENDPOINT_NAME = "YarnScheduler"
}
