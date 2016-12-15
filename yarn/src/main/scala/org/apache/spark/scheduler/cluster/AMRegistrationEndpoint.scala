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

import scala.collection.mutable

import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcAddress, RpcEndpointRef, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.RegisterClusterManager

private[spark] class AMRegistrationEndpoint(override val rpcEnv: RpcEnv)
    extends ThreadSafeRpcEndpoint with Logging {

  private var amEndpoint: Option[RpcEndpointRef] = None

  private val registrationListeners = mutable.Buffer.empty[AMEventListener]

  def subscribeToAMRegistration(listener: AMEventListener): Unit = {
    registrationListeners += listener
  }

  override def receive: PartialFunction[Any, Unit] = {
    case RegisterClusterManager(am) =>
      logInfo(s"ApplicationMaster registered as $am")
      amEndpoint = Option(am)
      registrationListeners.foreach(_.onConnected(am))
  }

  override def onDisconnected(remoteAddress: RpcAddress): Unit = {
    registrationListeners.foreach(_.onDisconnected(remoteAddress))
    amEndpoint = None
  }
}

private[spark] object AMRegistrationEndpoint {
  val ENDPOINT_NAME = "AMRegistration"
}

private[spark] trait AMEventListener {
  def onConnected(am: RpcEndpointRef): Unit
  def onDisconnected(remoteAddress: RpcAddress): Unit
}
