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

package org.apache.spark.scheduler

import scala.collection.mutable

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcCallContext, RpcEndpoint, RpcEndpointRef, RpcEnv}

private sealed trait OutputCommitCoordinationMessage extends Serializable

private case object StopCoordinator extends OutputCommitCoordinationMessage
private case class AskPermissionToCommitOutput(stage: Int, partition: Int, attemptNumber: Int)
private case class InformCommitDone(stage: Int, partition: Int, attemptNumber: Int)

object CommitStatus extends Enumeration {
  val NotCommitted, Committing, Committed = Value
}

private case class CommitState(attempt: Int, time: Long, state: CommitStatus.Value)


/**
 * Authority that decides whether tasks can commit output to HDFS. Uses a "first committer wins"
 * policy.
 *
 * OutputCommitCoordinator is instantiated in both the drivers and executors. On executors, it is
 * configured with a reference to the driver's OutputCommitCoordinatorEndpoint, so requests to
 * commit output will be forwarded to the driver's OutputCommitCoordinator.
 *
 * This class was introduced in SPARK-4879; see that JIRA issue (and the associated pull requests)
 * for an extensive design discussion.
 */
private[spark] class OutputCommitCoordinator(conf: SparkConf, isDriver: Boolean) extends Logging {

  import CommitStatus._

  // Initialized by SparkEnv
  var coordinatorRef: Option[RpcEndpointRef] = None

  private type StageId = Int
  private type PartitionId = Int
  private type TaskAttemptNumber = Int

  private val NO_AUTHORIZED_COMMITTER: TaskAttemptNumber = -1
  // Timeout to release the lock on a task in milliseconds, defaults to 120 seconds
  private val MAX_WAIT_FOR_COMMIT = conf.getLong(
    "spark.scheduler.outputCommitCoordinator.maxWaitTime", 120000L
  ) * 1e6.toLong

  /**
   * Map from active stages's id => partition id => task attempt with exclusive lock on committing
   * output for that partition.
   *
   * Entries are added to the top-level map when stages start and are removed they finish
   * (either successfully or unsuccessfully).
   *
   * Access to this map should be guarded by synchronizing on the OutputCommitCoordinator instance.
   */
  private val authorizedCommittersByStage = mutable.Map[StageId, Array[CommitState]]()

  /**
   * Returns whether the OutputCommitCoordinator's internal data structures are all empty.
   */
  def isEmpty: Boolean = {
    authorizedCommittersByStage.isEmpty
  }

  /**
   * Called by tasks to ask whether they can commit their output to HDFS.
   *
   * If a task attempt has been authorized to commit, then all other attempts to commit
   * the same task within spark.scheduler.outputCommitCoordinator.maxWaitTime
   * will be denied. If the authorized task attempt fails (e.g. due to its executor being lost),
   * then a subsequent task attempt may be authorized to commit its output.
   *
   * @param stage the stage number
   * @param partition the partition number
   * @param attemptNumber how many times this task has been attempted
   *                      (see [[TaskContext.attemptNumber()]])
   * @return true if this task is authorized to commit, false otherwise
   */
  def canCommit(
      stage: StageId,
      partition: PartitionId,
      attemptNumber: TaskAttemptNumber): Boolean = {
    val msg = AskPermissionToCommitOutput(stage, partition, attemptNumber)
    coordinatorRef match {
      case Some(endpointRef) =>
        endpointRef.askWithRetry[Boolean](msg)
      case None =>
        logError(
          "canCommit called after coordinator was stopped (is SparkEnv shutdown in progress)?")
        false
    }
  }

  /**
   * Called by tasks to inform their commit is done.
   *
   * @param stage the stage number
   * @param partition the partition number
   * @param attemptNumber how many times this task has been attempted
   *                      (see [[TaskContext.attemptNumber()]])
   * @return true if this task is authorized to commit, false otherwise
   */
  def commitDone(
                 stage: StageId,
                 partition: PartitionId,
                 attemptNumber: TaskAttemptNumber): Boolean = {
    val msg = InformCommitDone(stage, partition, attemptNumber)
    coordinatorRef match {
      case Some(endpointRef) =>
        endpointRef.askWithRetry[Boolean](msg)
      case None =>
        logError(
          "commitDone called after coordinator was stopped (is SparkEnv shutdown in progress)?")
        false
    }
  }


  /**
   * Called by the DAGScheduler when a stage starts.
   *
   * @param stage the stage id.
   * @param maxPartitionId the maximum partition id that could appear in this stage's tasks (i.e.
   *                       the maximum possible value of `context.partitionId`).
   */
  private[scheduler] def stageStart(
      stage: StageId,
      maxPartitionId: Int): Unit = {
    val arr = Array.fill[CommitState](maxPartitionId + 1)(CommitState(
      NO_AUTHORIZED_COMMITTER, 0, NotCommitted))
    synchronized {
      authorizedCommittersByStage(stage) = arr
    }
  }

  // Called by DAGScheduler
  private[scheduler] def stageEnd(stage: StageId): Unit = synchronized {
    authorizedCommittersByStage.remove(stage)
  }

  // Called by DAGScheduler
  private[scheduler] def taskCompleted(
      stage: StageId,
      partition: PartitionId,
      attemptNumber: TaskAttemptNumber,
      reason: TaskEndReason): Unit = synchronized {
    val authorizedCommitters = authorizedCommittersByStage.getOrElse(stage, {
      logDebug(s"Ignoring task completion for completed stage")
      return
    })
    reason match {
      case Success =>
      // The task output has been committed successfully
      case denied: TaskCommitDenied =>
        logInfo(s"Task was denied committing, stage: $stage, partition: $partition, " +
          s"attempt: $attemptNumber")
      case otherReason =>
        if (authorizedCommitters(partition) == attemptNumber) {
          logDebug(s"Authorized committer (attemptNumber=$attemptNumber, stage=$stage, " +
            s"partition=$partition) failed; clearing lock")
          authorizedCommitters(partition) = CommitState(NO_AUTHORIZED_COMMITTER, 0, NotCommitted)
        }
    }
  }

  def stop(): Unit = synchronized {
    if (isDriver) {
      coordinatorRef.foreach(_ send StopCoordinator)
      coordinatorRef = None
      authorizedCommittersByStage.clear()
    }
  }

  // Marked private[scheduler] instead of private so this can be mocked in tests
  private[scheduler] def handleAskPermissionToCommit(
      stage: StageId,
      partition: PartitionId,
      attemptNumber: TaskAttemptNumber): Boolean = synchronized {
    authorizedCommittersByStage.get(stage) match {
      case Some(authorizedCommitters) =>
        authorizedCommitters(partition) match {
          case CommitState(NO_AUTHORIZED_COMMITTER, _, NotCommitted) =>
            logDebug(s"Authorizing attemptNumber=$attemptNumber to commit for stage=$stage, " +
              s"partition=$partition")
            authorizedCommitters(partition) = CommitState(
              attemptNumber, System.nanoTime(), Committing
            )
            true
          case CommitState(existingCommitter, _, Committed) =>
            logWarning(s"Denying attemptNumber=$attemptNumber to commit for stage=$stage, " +
              s"partition=$partition; it is already committed")
            false
          case CommitState(existingCommitter, startTime, Committing)
            if System.nanoTime() - startTime > MAX_WAIT_FOR_COMMIT =>
            logWarning(s"Authorizing attemptNumber=$attemptNumber to commit for stage=$stage, " +
              s"partition=$partition; maxWaitTime=$MAX_WAIT_FOR_COMMIT " +
              s"reached and prior lock released for attemptId=$existingCommitter")
            authorizedCommitters(partition) = CommitState(
              attemptNumber, System.nanoTime(), Committing
            )
            true
          case CommitState(existingCommitter, startTime, _) =>
            logDebug(s"Denying attemptNumber=$attemptNumber to commit for stage=$stage, " +
              s"partition=$partition; existingCommitter = $existingCommitter with " +
              s"startTime=$startTime and currentTime=${System.nanoTime()}")
            false
        }
      case None =>
        logDebug(s"Stage $stage has completed, so not allowing attempt number $attemptNumber of" +
          s"partition $partition to commit")
        false
    }
  }

  private[scheduler] def handleInformCommitDone(
      stage: StageId,
      partition: PartitionId,
      attemptNumber: TaskAttemptNumber): Boolean = synchronized {
    authorizedCommittersByStage.get(stage) match {
      case Some(authorizedCommitters) =>
        authorizedCommitters(partition) match {
          case CommitState(existingCommitter, startTime, Committing)
            if attemptNumber == existingCommitter =>
            logDebug(s"Marking attemptNumber=$attemptNumber for stage=$stage, " +
              s"partition=$partition as committed")
            authorizedCommitters(partition) = CommitState(
              attemptNumber, startTime, Committed
            )
            true
          case CommitState(committer, startTime, status) =>
            logWarning(s"Bad state on attemptNumber=$attemptNumber for stage=$stage, " +
              s"partition=$partition: committer=$committer, start=$startTime, status=$status")
            false
        }
      case None =>
        logWarning(s"Stage $stage has completed but received commit completed from " +
          s"attemptNumber=$attemptNumber partition=$partition")
        false
    }
  }

}

private[spark] object OutputCommitCoordinator {

  // This endpoint is used only for RPC
  private[spark] class OutputCommitCoordinatorEndpoint(
      override val rpcEnv: RpcEnv, outputCommitCoordinator: OutputCommitCoordinator)
    extends RpcEndpoint with Logging {

    logDebug("init") // force eager creation of logger

    override def receive: PartialFunction[Any, Unit] = {
      case StopCoordinator =>
        logInfo("OutputCommitCoordinator stopped!")
        stop()
    }

    override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
      case AskPermissionToCommitOutput(stage, partition, attemptNumber) =>
        context.reply(
          outputCommitCoordinator.handleAskPermissionToCommit(stage, partition, attemptNumber))
      case InformCommitDone(stage, partition, attemptNumber) =>
        context.reply(
          outputCommitCoordinator.handleInformCommitDone(stage, partition, attemptNumber))
    }
  }
}
