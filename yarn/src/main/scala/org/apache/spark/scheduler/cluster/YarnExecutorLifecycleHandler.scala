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

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

import org.apache.spark.SparkConf
import org.apache.spark.clustermanager.plugins.scheduler.ClusterManagerExecutorLifecycleHandler
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcAddress, RpcEndpointRef}
import org.apache.spark.scheduler.{ExecutorLossReason, SlaveLost}
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.GetExecutorLossReason
import org.apache.spark.util.RpcUtils

private[spark] class YarnExecutorLifecycleHandler(
    amRegistrationEndpoint: AMRegistrationEndpoint,
    conf: SparkConf) extends ClusterManagerExecutorLifecycleHandler with Logging {

  private var amEndpoint: Option[RpcEndpointRef] = None
  private implicit val askTimeout = RpcUtils.askRpcTimeout(conf)

  amRegistrationEndpoint.subscribeToAMRegistration(new AMEventListener {
    override def onConnected(am: RpcEndpointRef): Unit = {
      amEndpoint = Some(am)
    }

    override def onDisconnected(remoteAddress: RpcAddress): Unit = {
      amEndpoint = None
    }
  })

  override def executorLossReasonRetriever(implicit ec: ExecutionContext)
      : Option[((String, RpcAddress) => Future[ExecutorLossReason])] = {
    Some(new GetExecutorLossReasonFunction(ec))
  }

  private class GetExecutorLossReasonFunction(
      val ec: ExecutionContext
    ) extends ((String, RpcAddress) => Future[ExecutorLossReason]) {

    override def apply(executorId: String, executorAddress: RpcAddress)
        : Future[ExecutorLossReason] = {
      amEndpoint match {
        case Some(am) =>
          val lossReasonRequest = GetExecutorLossReason(executorId)
          am.ask[ExecutorLossReason](lossReasonRequest, askTimeout)
            .recover {
              case NonFatal(e) =>
                logWarning(s"Attempted to get executor loss reason" +
                  s" for executor id ${executorId} at RPC address ${executorAddress}," +
                  s" but got no response. Marking as slave lost.", e)
                SlaveLost()
            }(ec)
        case None =>
          logWarning("Attempted to check for an executor loss reason" +
            " before the AM has registered!")
          Future.successful(SlaveLost("AM is not yet registered."))
      }
    }
  }
}

