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
import org.apache.spark.deploy.yarn.{ApplicationMaster, YarnSparkHadoopUtil}
import org.apache.spark.rpc.RpcEnv
import org.apache.spark.scheduler.SchedulerBackendExecutorLifecycleManager

private[spark] class YarnClusterExecutorProvider(
    conf: SparkConf,
    executorLifecycleManager: SchedulerBackendExecutorLifecycleManager,
    rpcEnv: RpcEnv,
    sc: SparkContext)
  extends YarnExecutorProvider(conf, executorLifecycleManager, rpcEnv, sc) {

  override def start() {
    val attemptId = ApplicationMaster.getAttemptId
    bindToYarn(attemptId.getApplicationId(), Some(attemptId))
    super.start()
    totalExpectedExecutors = YarnSparkHadoopUtil.getInitialTargetExecutorNumber(sc.conf)
  }

}
