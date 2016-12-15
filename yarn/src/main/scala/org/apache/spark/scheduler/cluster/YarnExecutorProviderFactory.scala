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

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.clustermanager.plugins.scheduler.ClusterManagerExecutorProviderFactory
import org.apache.spark.rpc.RpcEnv
import org.apache.spark.scheduler.SchedulerBackendHooks

private[spark] class YarnExecutorProviderFactory(
    amRegistrationEndpoint: AMRegistrationEndpoint, deployMode: String)
  extends ClusterManagerExecutorProviderFactory[YarnExecutorProvider] {

  override def newClusterManagerExecutorProvider(
      conf: SparkConf,
      schedulerBackendHooks: SchedulerBackendHooks,
      rpcEnv: RpcEnv,
      sc: SparkContext): YarnExecutorProvider = {
    deployMode.toLowerCase match {
      case "client" =>
        new YarnClientExecutorProvider(conf, schedulerBackendHooks, amRegistrationEndpoint,
          rpcEnv, sc)
      case "cluster" =>
        new YarnClusterExecutorProvider(conf, schedulerBackendHooks, amRegistrationEndpoint,
          rpcEnv, sc)
      case invalid => throw new IllegalArgumentException("Cannot create an executor provider" +
        s" for deploy mode $deployMode")
    }
  }
}
