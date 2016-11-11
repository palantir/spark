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
package org.apache.spark.scheduler.cluster.kubernetes

import java.io.{File, FileInputStream}
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.Executors

import com.google.common.collect.Iterables
import com.google.common.util.concurrent.{SettableFuture, ThreadFactoryBuilder}
import io.fabric8.kubernetes.api.model.{ContainerPort, ContainerPortBuilder, EnvVar, EnvVarBuilder, ReplicationControllerBuilder}
import io.fabric8.kubernetes.api.model.extensions.DaemonSet
import io.fabric8.kubernetes.client.{ConfigBuilder, DefaultKubernetesClient, KubernetesClient, KubernetesClientException, Watcher}
import io.fabric8.kubernetes.client.Watcher.Action
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ExecutionContext, Future}
import scala.io.Source

import org.apache.spark.{SparkContext, SparkException}
import org.apache.spark.rpc.{RpcCallContext, RpcEndpointAddress, RpcEnv}
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.{RegisterExecutor, RetrieveSparkProps}
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend
import org.apache.spark.scheduler.TaskSchedulerImpl
import org.apache.spark.util.Utils

private[spark] class KubernetesClusterSchedulerBackend(
    scheduler: TaskSchedulerImpl,
    val sc: SparkContext)
  extends CoarseGrainedSchedulerBackend(scheduler, sc.env.rpcEnv) {

  import KubernetesClusterSchedulerBackend._

  private val EXECUTOR_MODIFICATION_LOCK = new Object
  private val runningExecutorControllers = new mutable.HashMap[String, String]
  private val inactiveExecutorControllers = new mutable.Queue[String]

  private val kubernetesMaster = conf
      .getOption("spark.kubernetes.master")
      .getOrElse(
        throw new SparkException("Kubernetes master must be specified in kubernetes mode."))

  private val executorDockerImage = conf
      .get("spark.kubernetes.executor.docker.image", s"spark-executor:${sc.version}")

  private val kubernetesNamespace = conf
      .getOption("spark.kubernetes.namespace")
      .getOrElse(
        throw new SparkException("Kubernetes namespace must be specified in kubernetes mode."))

  private val kubernetesAppId = conf.get("spark.kubernetes.app.id", sc.applicationId)

  private val executorPort = conf.get("spark.executor.port", DEFAULT_STATIC_PORT.toString).toInt

  /**
   * Allows for specifying a custom replication controller for the executor runtime. This should
   * only be used if the user really knows what they are doing. Allows for custom behavior on the
   * executors themselves, or for loading extra containers into the executor pods.
   */
  private val executorCustomSpecFile = conf.getOption("spark.kubernetes.executor.custom.spec.file")
  private val executorCustomSpecExecutorContainerName = executorCustomSpecFile.map(_ =>
    conf
      .getOption("spark.kubernetes.executor.custom.spec.container.name")
      .getOrElse(throw new SparkException("When using a custom replication controller spec" +
      " for executors, the name of the container that the executor will run in must be" +
      " specified via spark.kubernetes.executor.custom.spec.container.name")))

  private val blockmanagerPort = conf
      .get("spark.blockmanager.port", DEFAULT_BLOCKMANAGER_PORT.toString)
      .toInt

  private val kubernetesDriverServiceName = conf
      .getOption("spark.kubernetes.driver.service.name")
      .getOrElse(
        throw new SparkException("Must specify the service name the driver is running with"))

  private val executorMemory = conf.getOption("spark.executor.memory").getOrElse("1g")

  private val executorCores = conf.getOption("spark.executor.cores").getOrElse("1")

  private implicit val requestExecutorContext = ExecutionContext.fromExecutorService(
      Executors.newCachedThreadPool(
          new ThreadFactoryBuilder()
              .setDaemon(true)
              .setNameFormat("kubernetes-executor-requests")
              .build))

  private val kubernetesClient = createKubernetesClient()

  private val externalShuffleServiceEnabled = conf.getBoolean(
    "spark.shuffle.service.enabled", defaultValue = false)

  override val minRegisteredRatio =
    if (conf.getOption("spark.scheduler.minRegisteredResourcesRatio").isEmpty) {
      0.8
    } else {
      super.minRegisteredRatio
    }

  protected var totalExpectedExecutors = new AtomicInteger(0)
  private val maybeShuffleService = initializeShuffleService()
  private val driverUrl = RpcEndpointAddress(
    System.getenv(s"${convertToEnvMode(kubernetesDriverServiceName)}_SERVICE_HOST"),
    sc.getConf.get("spark.driver.port").toInt,
    CoarseGrainedSchedulerBackend.ENDPOINT_NAME).toString

  private def convertToEnvMode(value: String): String =
    value.toUpperCase.map { c => if (c == '-') '_' else c }

  private val initialExecutors = getInitialTargetExecutorNumber(1)

  override def sufficientResourcesRegistered(): Boolean = {
    totalRegisteredExecutors.get() >= initialExecutors * minRegisteredRatio
  }

  private def createKubernetesClient(): KubernetesClient = {
    var clientConfigBuilder = new ConfigBuilder()
        .withApiVersion("v1")
        .withMasterUrl(kubernetesMaster)
        .withNamespace(kubernetesNamespace)

    if (CA_CERT_FILE.exists) {
      clientConfigBuilder = clientConfigBuilder.withCaCertFile(CA_CERT_FILE.getAbsolutePath)
    }

    if (API_SERVER_TOKEN.exists) {
      clientConfigBuilder = clientConfigBuilder.withOauthToken(
          Source.fromFile(API_SERVER_TOKEN).mkString)
    }
    new DefaultKubernetesClient(clientConfigBuilder.build)
  }

  override def start(): Unit = {
    super.start()
    if (!Utils.isDynamicAllocationEnabled(sc.conf)) {
      doRequestTotalExecutors(initialExecutors)
    }
  }

  private def allocateNewExecutorReplicationController(): String = {
    val executorKubernetesId = UUID.randomUUID().toString.replaceAll("-", "")
    val name = s"exec$executorKubernetesId"
    val selectors = Map(SPARK_EXECUTOR_SELECTOR -> executorKubernetesId,
      SPARK_APP_SELECTOR -> kubernetesAppId).asJava
    val resolvedName = executorCustomSpecFile match {
      case Some(filePath) =>
        val file = new File(filePath)
        if (!file.exists()) {
          throw new SparkException(s"Custom executor spec file not found at $filePath")
        }
        val providedController = Utils.tryWithResource(new FileInputStream(file)) { is =>
          kubernetesClient.replicationControllers().load(is)
        }
        val resolvedContainers = providedController.get.getSpec.getTemplate.getSpec
          .getContainers.asScala
        var foundExecutorContainer = false
        for (container <- resolvedContainers) {
          if (container.getName == executorCustomSpecExecutorContainerName.get) {
            foundExecutorContainer = true
            val resolvedEnv = new ArrayBuffer[EnvVar]
            resolvedEnv ++= container.getEnv.asScala
            resolvedEnv += new EnvVarBuilder()
              .withName("SPARK_EXECUTOR_PORT")
              .withValue(executorPort.toString)
              .build()
            resolvedEnv += new EnvVarBuilder()
              .withName("SPARK_DRIVER_URL")
              .withValue(driverUrl)
              .build()
            resolvedEnv += new EnvVarBuilder()
              .withName("SPARK_EXECUTOR_CORES")
              .withValue(executorCores)
              .build()
            resolvedEnv += new EnvVarBuilder()
              .withName("SPARK_EXECUTOR_MEMORY")
              .withValue(executorMemory)
              .build()
            resolvedEnv += new EnvVarBuilder()
              .withName("SPARK_APPLICATION_ID")
              .withValue(kubernetesAppId)
              .build()
            container.setEnv(resolvedEnv.asJava)
            val resolvedPorts = new ArrayBuffer[ContainerPort]
            resolvedPorts ++= container.getPorts.asScala
            resolvedPorts += new ContainerPortBuilder()
              .withName(EXECUTOR_PORT_NAME)
              .withContainerPort(executorPort)
              .build()
            resolvedPorts += new ContainerPortBuilder()
              .withName(BLOCK_MANAGER_PORT_NAME)
              .withContainerPort(blockmanagerPort)
              .build()
            container.setPorts(resolvedPorts.asJava)
          }
        }
        if (!foundExecutorContainer) {
          throw new SparkException("Expected container"
            + s" ${executorCustomSpecExecutorContainerName.get}" +
            " to be provided as the executor container in the custom" +
            " executor replication controller, but it was not found in" +
            " the provided spec file.")
        }
        val editedController = new ReplicationControllerBuilder(providedController.get())
          .editMetadata()
            .withName(name)
            .addToLabels(selectors)
            .endMetadata()
          .editSpec()
            .withReplicas(1)
            .addToSelector(selectors)
            .editTemplate()
              .editMetadata()
                .addToLabels(selectors)
                .endMetadata()
              .editSpec()
                .withContainers(resolvedContainers.asJava)
                .endSpec()
              .endTemplate()
            .endSpec()
          .build()
        kubernetesClient.replicationControllers().create(editedController).getMetadata.getName
      case None =>
        kubernetesClient.replicationControllers().createNew()
          .withNewMetadata()
            .withName(name)
            .withLabels(selectors)
            .endMetadata()
          .withNewSpec()
            .withReplicas(1)
            .withSelector(selectors)
            .withNewTemplate()
              .withNewMetadata()
                .withName(s"exec-$kubernetesAppId-pod")
                .withLabels(selectors)
                .endMetadata()
              .withNewSpec()
                .addNewContainer()
                  .withName(s"exec-$kubernetesAppId-container")
                  .withImage(executorDockerImage)
                  .withImagePullPolicy("IfNotPresent")
                  .addNewEnv()
                    .withName("SPARK_EXECUTOR_PORT")
                    .withValue(executorPort.toString)
                    .endEnv()
                  .addNewEnv()
                    .withName("SPARK_DRIVER_URL")
                    .withValue(driverUrl)
                    .endEnv()
                  .addNewEnv()
                    .withName("SPARK_EXECUTOR_CORES")
                    .withValue(executorCores)
                    .endEnv()
                  .addNewEnv()
                    .withName("SPARK_EXECUTOR_MEMORY")
                    .withValue(executorMemory)
                    .endEnv()
                  .addNewEnv()
                    .withName("SPARK_APPLICATION_ID")
                    .withValue(kubernetesAppId).endEnv()
                  .addNewPort()
                    .withName(EXECUTOR_PORT_NAME)
                    .withContainerPort(executorPort.toInt)
                    .endPort()
                  .addNewPort()
                    .withName(BLOCK_MANAGER_PORT_NAME)
                    .withContainerPort(blockmanagerPort.toInt)
                    .endPort()
                  .endContainer()
                .endSpec()
              .endTemplate()
            .endSpec()
          .done().getMetadata.getName
    }
    resolvedName
  }

  private def initializeShuffleService(): Option[DaemonSet] = {
    if (!externalShuffleServiceEnabled) {
      None
    } else {
      val shuffleServicePort = conf.getInt("spark.shuffle.service.port", 7337)
      val shuffleServiceDockerImage = conf
        .getOption("spark.kubernetes.shuffle.service.docker.image")
        .getOrElse(s"spark-shuffle-service:${sc.version}")
      val selectors = Map(SPARK_SHUFFLE_SERVICE_SELECTOR -> kubernetesAppId).asJava
      val daemonSetName = s"shuffle-service-$kubernetesAppId"

      val shuffleServiceReady = SettableFuture.create[Boolean]()
      val watch = kubernetesClient
        .extensions()
        .daemonSets()
        .withLabels(selectors)
        .watch(new Watcher[DaemonSet] {
          override def eventReceived(action: Action, daemonSet: DaemonSet): Unit = {
            val name = daemonSet.getMetadata.getName
            action match {
              case a @ (Action.ADDED | Action.MODIFIED) if name == daemonSetName =>
                if (daemonSet.getStatus.getCurrentNumberScheduled
                      >= daemonSet.getStatus.getDesiredNumberScheduled) {
                  synchronized {
                    shuffleServiceReady.set(true)
                  }
                }
              case _ =>
            }
          }

          override def onClose(cause: KubernetesClientException): Unit = {
            synchronized {
              if (!shuffleServiceReady.isDone) {
                shuffleServiceReady.setException(
                  new IllegalStateException("Received closed event for shuffle service"
                    + " daemon set.", cause))
              }
            }
          }
        })

      Utils.tryWithResource(watch)(_ => {
        val shuffleServiceDaemonSet = kubernetesClient
          .extensions()
          .daemonSets()
          .createNew()
            .withNewMetadata()
              .withName(daemonSetName)
              .withLabels(selectors)
              .endMetadata()
            .withNewSpec()
              .withNewSelector()
                .withMatchLabels(selectors)
                .endSelector()
              .withNewTemplate()
                .withNewMetadata()
                .withName(s"shuffle-service-$kubernetesAppId-pod")
                .withLabels(selectors)
                .endMetadata()
              .withNewSpec()
                .addNewContainer()
                  .withName(s"shuffle-service-$kubernetesAppId-container")
                  .withImage(shuffleServiceDockerImage)
                  .withImagePullPolicy("IfNotPresent")
                  .addNewPort().withContainerPort(shuffleServicePort).endPort()
                  .addNewEnv()
                    .withName("SPARK_SHUFFLE_SERVICE_PORT")
                    .withValue(shuffleServicePort.toString)
                    .endEnv()
                  .endContainer()
                .endSpec()
              .endTemplate()
            .endSpec()
          .done()
        shuffleServiceReady.get
        Some(shuffleServiceDaemonSet)
      })
    }
  }

  override def doRequestTotalExecutors(requestedTotal: Int): Future[Boolean] = Future[Boolean] {
    EXECUTOR_MODIFICATION_LOCK.synchronized {
      if (requestedTotal > totalExpectedExecutors.get) {
        logInfo(s"Requesting ${requestedTotal - totalExpectedExecutors.get}"
          + s" additional executors, expecting total $requestedTotal and currently" +
          s" expected ${totalExpectedExecutors.get}")
        for (i <- 0 until (requestedTotal - totalExpectedExecutors.get)) {
          if (inactiveExecutorControllers.isEmpty) {
            allocateNewExecutorReplicationController()
          } else {
            val executor = inactiveExecutorControllers.dequeue
            kubernetesClient.replicationControllers().withName(executor).scale(1)
          }
        }
      }
      totalExpectedExecutors.set(requestedTotal)
    }
    true
  }

  override def doKillExecutors(executorIds: Seq[String]): Future[Boolean] = Future[Boolean] {
    EXECUTOR_MODIFICATION_LOCK.synchronized {
      executorIds.foreach(executor => {
        runningExecutorControllers.remove(executor) match {
          case Some(name) =>
            kubernetesClient.replicationControllers().withName(name).scale(0, true)
            inactiveExecutorControllers += name
          case None =>
            throw new IllegalStateException(s"Trying to remove executor $executor before it" +
              s" has registered with the scheduler.")
        }
      })
    }
    true
  }

  private def getInitialTargetExecutorNumber(defaultNumExecutors: Int = 1): Int = {
    if (Utils.isDynamicAllocationEnabled(conf)) {
      val minNumExecutors = conf.getInt("spark.dynamicAllocation.minExecutors", 0)
      val initialNumExecutors = Utils.getDynamicAllocationInitialExecutors(conf)
      val maxNumExecutors = conf.getInt("spark.dynamicAllocation.maxExecutors", 1)
      require(initialNumExecutors >= minNumExecutors && initialNumExecutors <= maxNumExecutors,
        s"initial executor number $initialNumExecutors must between min executor number " +
          s"$minNumExecutors and max executor number $maxNumExecutors")

      initialNumExecutors
    } else {
      conf.getInt("spark.executor.instances", defaultNumExecutors)
    }
  }

  override def stop(): Unit = {
    // TODO investigate why Utils.tryLogNonFatalError() doesn't work in this context.
    // When using Utils.tryLogNonFatalError some of the code fails but without any logs or
    // indication as to why.
    try {
      val allControllers = kubernetesClient
        .replicationControllers()
        .withLabel(SPARK_APP_SELECTOR, kubernetesAppId)
        .withLabel(SPARK_EXECUTOR_SELECTOR)
        .list()
        .getItems
        .asScala
      allControllers.map(_.getMetadata.getName).foreach(controller => {
        kubernetesClient.replicationControllers.withName(controller).cascading(true).delete()
      })
    } catch {
      case e: Throwable => logError("Uncaught exception while shutting down controllers.", e)
    }

    maybeShuffleService.foreach(service => {
      try {
        kubernetesClient
          .extensions()
          .daemonSets
          .withName(service.getMetadata.getName)
          .cascading(true)
          .delete()
        kubernetesClient
          .pods()
          .withLabels(service.getSpec.getSelector.getMatchLabels)
          .delete()
      } catch {
        case e: Throwable => logError("Uncaught exception while shutting down shuffle service.", e)
      }
    })

    try {
      kubernetesClient.services().withName(kubernetesDriverServiceName).delete()
    } catch {
      case e: Throwable => logError("Uncaught exception while shutting down driver service.", e)
    }

    try {
      kubernetesClient.close()
    } catch {
      case e: Throwable => logError("Uncaught exception closing Kubernetes client.", e)
    }
    super.stop()
  }

  override def createDriverEndpoint(properties: Seq[(String, String)]): DriverEndpoint = {
    new KubernetesDriverEndpoint(rpcEnv, properties)
  }

  private class KubernetesDriverEndpoint(override val rpcEnv: RpcEnv,
      sparkProperties: Seq[(String, String)])
    extends DriverEndpoint(rpcEnv, sparkProperties) {

    override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
      new PartialFunction[Any, Unit]() {
        override def isDefinedAt(x: Any): Boolean = {
          x match {
            case RetrieveSparkProps(executorHostname) => true
            case RegisterExecutor(executorId, executorRef, hostname, cores, logUrls) => true
            case _ => false
          }
        }

        override def apply(v1: Any): Unit = {
          v1 match {
            case RetrieveSparkProps(executorHostname) =>
              var resolvedProperties = sparkProperties
              maybeShuffleService.foreach(service => {
                val executorPod = kubernetesClient
                  .pods()
                  .withLabel(SPARK_APP_SELECTOR, kubernetesAppId)
                  .list()
                  .getItems
                  .asScala
                  .filter(_.getMetadata.getName == executorHostname)
                  .head
                val shuffleServiceForPod = kubernetesClient
                  .pods()
                  .withLabels(service.getSpec.getSelector.getMatchLabels)
                  .list()
                  .getItems
                  .asScala
                  .filter(_.getStatus.getHostIP == executorPod.getStatus.getHostIP)
                  .head
                resolvedProperties = resolvedProperties ++ Seq(
                  ("spark.shuffle.service.host", shuffleServiceForPod.getStatus.getPodIP))
              })
              context.reply(resolvedProperties)
            case RegisterExecutor(executorId, executorRef, hostname, cores, logUrls) =>
              doRegisterExecutor(context, executorId, executorRef, hostname, cores, logUrls)
              EXECUTOR_MODIFICATION_LOCK.synchronized {
                val podSelectorId = kubernetesClient.pods()
                  .withName(hostname)
                  .get
                  .getMetadata
                  .getLabels
                  .get(SPARK_EXECUTOR_SELECTOR)
                val executorReplicationControllerName = Iterables.getOnlyElement(
                  kubernetesClient.replicationControllers()
                    .withLabel(SPARK_EXECUTOR_SELECTOR, podSelectorId)
                    .list()
                  .getItems).getMetadata.getName
                runningExecutorControllers.put(executorId, executorReplicationControllerName)
              }
          }
        }
      }.orElse(super.receiveAndReply(context))
    }
  }
}

private object KubernetesClusterSchedulerBackend {
  private val API_SERVER_TOKEN = new File("/var/run/secrets/kubernetes.io/serviceaccount/token")
  private val CA_CERT_FILE = new File("/var/run/secrets/kubernetes.io/serviceaccount/ca.crt")
  private val SPARK_EXECUTOR_SELECTOR = "spark-exec"
  private val SPARK_APP_SELECTOR = "spark-app"
  private val SPARK_SHUFFLE_SERVICE_SELECTOR = "shuffle-svc"
  private val DEFAULT_STATIC_PORT = 10000
  private val DEFAULT_BLOCKMANAGER_PORT = 7079
  private val BLOCK_MANAGER_PORT_NAME = "blockmanager"
  private val EXECUTOR_PORT_NAME = "executor"
}
