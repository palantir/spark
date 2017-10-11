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

import java.io.DataInputStream
import java.io.InputStream
import java.io.OutputStream
import java.net.InetAddress
import java.net.ServerSocket
import java.net.Socket
import java.net.SocketException

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark._
import org.apache.spark.api.conda.CondaEnvironment.CondaSetupInstructions
import org.apache.spark.internal.Logging
import org.apache.spark.util.RedirectThread
import org.apache.spark.util.Utils

private[spark] abstract class CondaAwareWorkerFactory(
    requestedEnvVars: Map[String, String],
    condaInstructions: Option[CondaSetupInstructions])
  extends Logging {

  import CondaAwareWorkerFactory._

  protected def binary: String

  protected def getPidFromProcess(dataInputStream: DataInputStream): Int

  protected def getDaemonPort(dataInputStream: DataInputStream): Int

  protected def tellWorkerOurPort(outputStream: OutputStream, port: Int): Unit

  protected def createSimpleProcessBuilder(): ProcessBuilder

  protected def createDaemonProcessBuilder(): ProcessBuilder

  protected def handleDaemonStartException(e: Exception, stderr: String): Unit

  protected def killWorkerByPid(outputStream: OutputStream, pid: Int): Unit

  // Because forking processes from Java is expensive, we prefer to launch a single daemon and
  // tell it to fork new workers for our tasks. This daemon currently only works on UNIX-based
  // systems now because it uses signals for child management, so we can also fall back to
  // launching workers (pyspark/worker.py) directly.
  private[this] val useDaemon = !Utils.isWindows
  private[this] var daemon: Process = null
  private[this] val daemonHost = InetAddress.getByAddress(Array[Byte](127, 0, 0, 1))
  private[this] var daemonPort: Int = 0
  private[this] val daemonWorkers = new mutable.WeakHashMap[Socket, Int]()
  private[this] val idleWorkers = new mutable.Queue[Socket]()
  private[this] var lastActivity = 0L
  new MonitorThread().start()


  private[this] val simpleWorkers = new mutable.WeakHashMap[Socket, Process]()

  protected[this] lazy val condaEnv: Option[CondaEnvironment] = {
    // Set up conda environment if there are any conda packages requested
    condaInstructions.map(CondaEnvironmentManager.createCondaEnvironment)
  }

  protected[this] lazy val envVars: Map[String, String] = {
    condaEnv.map(_.activatedEnvironment(requestedEnvVars)).getOrElse(requestedEnvVars)
  }

  def create(): Socket = {
    if (useDaemon) {
      synchronized {
        if (idleWorkers.nonEmpty) {
          return idleWorkers.dequeue()
        }
      }
      createThroughDaemon()
    } else {
      createSimpleWorker()
    }
  }

  /**
   * Connect to a worker launched through pyspark/daemon.py, which forks python processes itself
   * to avoid the high cost of forking from Java. This currently only works on UNIX-based systems.
   */
  private def createThroughDaemon(): Socket = {

    def createSocket(): Socket = {
      val socket = new Socket(daemonHost, daemonPort)
      val pid = getPidFromProcess(new DataInputStream(socket.getInputStream))
      if (pid < 0) {
        throw new IllegalStateException("Python daemon failed to launch worker with code " + pid)
      }
      daemonWorkers.put(socket, pid)
      socket
    }

    synchronized {
      // Start the daemon if it hasn't been started
      startDaemon()

      // Attempt to connect, restart and retry once if it fails
      try {
        createSocket()
      } catch {
        case exc: SocketException =>
          logWarning("Failed to open socket to daemon:", exc)
          logWarning("Assuming that daemon unexpectedly quit, attempting to restart")
          stopDaemon()
          startDaemon()
          createSocket()
      }
    }
  }
  /**
   * Launch a worker by executing worker.py directly and telling it to connect to us.
   */
  private def createSimpleWorker(): Socket = {
    var serverSocket: ServerSocket = null
    try {
      serverSocket = new ServerSocket(0, 1, InetAddress.getByAddress(Array[Byte](127, 0, 0, 1)))

      // Create and start the worker
      val pb = createSimpleProcessBuilder()
      pb.environment().putAll(envVars.asJava)
      val worker = pb.start()

      // Redirect worker stdout and stderr
      redirectStreamsToStderr(worker.getInputStream, worker.getErrorStream)

      // Tell the worker our port
      tellWorkerOurPort(worker.getOutputStream, serverSocket.getLocalPort)

      // Wait for it to connect to our socket
      serverSocket.setSoTimeout(10000)
      try {
        val socket = serverSocket.accept()
        simpleWorkers.put(socket, worker)
        return socket
      } catch {
        case e: Exception =>
          throw new SparkException("Worker did not connect back in time", e)
      }
    } finally {
      if (serverSocket != null) {
        serverSocket.close()
      }
    }
    null
  }


  private def startDaemon() {
    synchronized {
      // Is it already running?
      if (daemon != null) {
        return
      }

      try {
        // Create and start the daemon
        val pb = createDaemonProcessBuilder()
        pb.environment().putAll(envVars.asJava)
        daemon = pb.start()

        val in = new DataInputStream(daemon.getInputStream)
        daemonPort = getDaemonPort(in)

        // Redirect daemon stdout and stderr
        redirectStreamsToStderr(in, daemon.getErrorStream)

      } catch {
        case e: Exception =>

          // If the daemon exists, wait for it to finish and get its stderr
          val stderr = Option(daemon)
                       .flatMap { d => Utils.getStderr(d, PROCESS_WAIT_TIMEOUT_MS) }
                       .getOrElse("")

          stopDaemon()

          handleDaemonStartException(e, stderr)
      }

      // Important: don't close daemon's stdin (daemon.getOutputStream) so it can correctly
      // detect our disappearance.
    }
  }

  /**
   * Redirect the given streams to our stderr in separate threads.
   */
  private def redirectStreamsToStderr(stdout: InputStream, stderr: InputStream) {
    try {
      new RedirectThread(stdout, System.err, "stdout reader for " + binary).start()
      new RedirectThread(stderr, System.err, "stderr reader for " + binary).start()
    } catch {
      case e: Exception =>
        logError("Exception in redirecting streams", e)
    }
  }

  /**
   * Monitor all the idle workers, kill them after timeout.
   */
  private class MonitorThread extends Thread(s"Idle Worker Monitor for $binary") {

    setDaemon(true)

    override def run() {
      while (true) {
        synchronized {
          if (lastActivity + IDLE_WORKER_TIMEOUT_MS < System.currentTimeMillis()) {
            cleanupIdleWorkers()
            lastActivity = System.currentTimeMillis()
          }
        }
        Thread.sleep(10000)
      }
    }
  }

  private def cleanupIdleWorkers() {
    while (idleWorkers.nonEmpty) {
      val worker = idleWorkers.dequeue()
      try {
        // the worker will exit after closing the socket
        worker.close()
      } catch {
        case e: Exception =>
          logWarning("Failed to close worker socket", e)
      }
    }
  }

  private def stopDaemon() {
    synchronized {
      if (useDaemon) {
        cleanupIdleWorkers()

        // Request shutdown of existing daemon by sending SIGTERM
        if (daemon != null) {
          daemon.destroy()
        }

        daemon = null
        daemonPort = 0
      } else {
        simpleWorkers.mapValues(_.destroy())
      }
    }
  }

  def stop() {
    stopDaemon()
  }

  def stopWorker(worker: Socket) {
    synchronized {
      if (useDaemon) {
        if (daemon != null) {
          daemonWorkers.get(worker).foreach { pid =>
            // tell daemon to kill worker by pid
            killWorkerByPid(daemon.getOutputStream, pid)
          }
        }
      } else {
        simpleWorkers.get(worker).foreach(_.destroy())
      }
    }
    worker.close()
  }

  def releaseWorker(worker: Socket) {
    if (useDaemon) {
      synchronized {
        lastActivity = System.currentTimeMillis()
        idleWorkers.enqueue(worker)
      }
    } else {
      // Cleanup the worker socket. This will also cause the Python worker to exit.
      try {
        worker.close()
      } catch {
        case e: Exception =>
          logWarning("Failed to close worker socket", e)
      }
    }
  }
}

private object CondaAwareWorkerFactory {
  val PROCESS_WAIT_TIMEOUT_MS = 10000
  val IDLE_WORKER_TIMEOUT_MS = 60000  // kill idle workers after 1 minute
}
