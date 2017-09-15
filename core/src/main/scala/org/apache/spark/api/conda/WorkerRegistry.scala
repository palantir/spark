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

package org.apache.spark.api.conda

import java.net.Socket

import scala.collection.mutable

import org.apache.spark.api.conda.CondaEnvironment.CondaSetupInstructions
import org.apache.spark.api.conda.WorkerRegistry.WorkerKey
import org.apache.spark.deploy.Common.Provenance

private[spark] class WorkerRegistry(factory: WorkerKey => CondaAwareWorkerFactory) {
  import WorkerRegistry._

  private val workers = new Guard(mutable.HashMap[WorkerKey, CondaAwareWorkerFactory]())

  def createWorker(key: WorkerKey): Socket = {
    workers.withValue(_.getOrElseUpdate(key, factory(key)).create())
  }

  def destroyWorker(key: WorkerKey, worker: Socket) {
    workers.withValue(_.get(key).foreach(_.stopWorker(worker)))
  }

  def releaseWorker(key: WorkerKey, worker: Socket) {
      workers.withValue(_.get(key).foreach(_.releaseWorker(worker)))
  }

  def foreach(f: (WorkerKey, CondaAwareWorkerFactory) => Unit): Unit = {
    workers.withValue(_.foreach(f.tupled))
  }
}

object WorkerRegistry {
  type UserProvidedExecutable = Option[String]
  type EnvVars = Map[String, String]

  case class WorkerKey(exec: UserProvidedExecutable,
                       envVars: EnvVars,
                       condaInstructions: Option[CondaSetupInstructions])

  /**
   * A locked object that helps prevent un-synchronized access.
   */
  private[WorkerRegistry] class Guard[T](value: T) {
    def withValue[U](f: T => U): U = {
      synchronized {
        f.apply(value)
      }
    }
  }
}
