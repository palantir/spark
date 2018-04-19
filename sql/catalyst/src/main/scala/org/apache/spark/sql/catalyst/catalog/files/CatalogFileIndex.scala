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

package org.apache.spark.sql.catalyst.catalog.files

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.StructType

/**
 * A FileIndex which is backed by catalog table.
 */
trait CatalogFileIndex extends FileIndex {

  val table: CatalogTable

  /**
   * Returns a [[FileIndex]] for this table restricted to the subset of partitions
   * specified by the given partition-pruning filters.
   *
   * @param filters partition-pruning filters
   */
  def filterPartitions(filters: Seq[Expression]): FileIndex

  override def partitionSchema: StructType = table.partitionSchema

  override def rootPaths: Seq[Path] = table.storage.locationUri.map(new Path(_)).toSeq

  override def listFiles(
      partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {
    filterPartitions(partitionFilters).listFiles(Nil, dataFilters)
  }

  override def inputFiles: Array[String] = filterPartitions(Nil).inputFiles
}
