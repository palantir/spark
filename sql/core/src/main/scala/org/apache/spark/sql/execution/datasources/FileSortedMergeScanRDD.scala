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

package org.apache.spark.sql.execution.datasources

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow

class FileSortedMergeScanRDD(
  @transient sparkSession: SparkSession,
  readFunction: (PartitionedFile) => Iterator[InternalRow],
  @transient filePartitions: Seq[FilePartition],
  val sortOrdering: Ordering[InternalRow]
) extends FileScanRDD(sparkSession, readFunction, filePartitions) {
  private val ignoreCorruptFiles = sparkSession.sessionState.conf.ignoreCorruptFiles
  private val ignoreMissingFiles = sparkSession.sessionState.conf.ignoreMissingFiles

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    val iterator = new FileSortedBucketScanIterator(split, context,
      ignoreCorruptFiles, ignoreMissingFiles, readFunction, sortOrdering)
    // Register an on-task-completion callback to close the input stream.
    context.addTaskCompletionListener[Unit](_ => iterator.close())

    iterator.asInstanceOf[Iterator[InternalRow]] // This is an erasure hack.
  }
}
