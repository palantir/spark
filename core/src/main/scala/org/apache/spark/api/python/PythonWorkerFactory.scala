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

package org.apache.spark.api.python

import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.OutputStream
import java.io.OutputStreamWriter
import java.nio.charset.StandardCharsets
import java.util

import org.apache.spark._
import org.apache.spark.api.conda.CondaAwareWorkerFactory
import org.apache.spark.api.conda.CondaEnvironment.CondaSetupInstructions

private[spark] class PythonWorkerFactory(requestedPythonExec: Option[String],
                                         requestedEnvVars: Map[String, String],
                                         condaInstructions: Option[CondaSetupInstructions])
    extends CondaAwareWorkerFactory(requestedEnvVars, condaInstructions) {

  override lazy val binary: String = {
    condaEnv.map { conda =>
      requestedPythonExec.foreach(exec => sys.error(s"It's forbidden to set the PYSPARK_PYTHON " +
        s"when using conda, but found: $exec"))

      conda.condaEnvDir + "/bin/python"
    }.orElse(requestedPythonExec)
     .getOrElse("python")
  }

  val pythonPath = PythonUtils.mergePythonPaths(
    PythonUtils.sparkPythonPath,
    envVars.getOrElse("PYTHONPATH", ""),
    sys.env.getOrElse("PYTHONPATH", ""))

  override def createSimpleProcessBuilder(): ProcessBuilder = {
    val pb = new ProcessBuilder(util.Arrays.asList(binary, "-m", "pyspark.worker"))
    val workerEnv = pb.environment()
    workerEnv.put("PYTHONPATH", pythonPath)
    // This is equivalent to setting the -u flag; we use it because ipython doesn't support -u:
    workerEnv.put("PYTHONUNBUFFERED", "YES")
    pb
  }

  override def createDaemonProcessBuilder(): ProcessBuilder = {
    val pb = new ProcessBuilder(util.Arrays.asList(binary, "-m", "pyspark.daemon"))
    val workerEnv = pb.environment()
    workerEnv.put("PYTHONPATH", pythonPath)
    // This is equivalent to setting the -u flag; we use it because ipython doesn't support -u:
    workerEnv.put("PYTHONUNBUFFERED", "YES")
    pb
  }

  override protected def getPidFromProcess(dataInputStream: DataInputStream): Int = {
    dataInputStream.readInt()
  }


  override protected def getDaemonPort(dataInputStream: DataInputStream): Int = {
    dataInputStream.readInt()
  }

  override protected def tellWorkerOurPort(outputStream: OutputStream, port: Int): Unit = {
    val out = new  OutputStreamWriter(outputStream, StandardCharsets.UTF_8)
    out.write(port + "\n")
    out.flush()
  }

  override protected def handleDaemonStartException(e: Exception, stderr: String): Unit = {
    if (stderr != "") {
      val formattedStderr = stderr.replace("\n", "\n  ")
      val errorMessage = s"""
                            |Error from python worker:
                            |  $formattedStderr
                            |PYTHONPATH was:
                            |  $pythonPath
                            |$e"""

      // Append error message from python daemon, but keep original stack trace
      val wrappedException = new SparkException(errorMessage.stripMargin)
      wrappedException.setStackTrace(e.getStackTrace)
      throw wrappedException
    } else {
      throw e
    }
  }

  override protected def killWorkerByPid(outputStream: OutputStream, pid: Int): Unit = {
    val output = new DataOutputStream(outputStream)
    output.writeInt(pid)
    output.flush()
  }
}
