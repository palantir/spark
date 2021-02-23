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

/**
 * Determines the conda create mode run to bootstrap the conda environment.
 */
sealed trait CondaBootstrapMode

object CondaBootstrapMode {

  /**
   * Solve mode runs conda create using the SAT solver.
   */
  case object Solve extends CondaBootstrapMode

  /**
   * File mode runs conda create using a spec file that specifies the package urls to install.
   */
  case object File extends CondaBootstrapMode

  def fromString(value: String): CondaBootstrapMode = {
    values().find(_.toString == value).getOrElse(
      throw new IllegalArgumentException(
        s"Unable to construct CondaBootStrapMode from string $value"))
  }

  def values(): Vector[CondaBootstrapMode] = {
    Vector(Solve, File)
  }
}
