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


package org.apache.spark.sql

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.HadoopReadOptions
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.junit.Assert.assertFalse
import org.scalatest.concurrent.{Signaler, ThreadSignaler, TimeLimits}
import org.scalatest.time.{Seconds, Span}

import org.apache.spark.sql.execution.datasources.parquet.ParquetTest
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Loose collection of additional tests for Palantir-specific additions.
 */
class PalantirSuite extends QueryTest
  with ParquetTest
  with SharedSparkSession
  with TimeLimits {

  import testImplicits._

  implicit val defaultSignaler: Signaler = ThreadSignaler

  /**
   * We have additional behavior in CollapseProject to avoid excessive alias substitution when
   * collapsing many Projects. Excessive substitution means that the expression tree inside a
   * single Project node becomes too large and causes OOMs or the optimizer to hang.
   */
  test("SPARK-26626: Alias substitution does not explode plan") {
    var df = Seq(1, 2, 3).toDF("a").withColumn("b", lit(10))
    1 to 15 foreach { i =>
      df = df.select((col("a") + col("b")).as("a"), (col("a") - col("b")).as("b"))
    }

    cancelAfter(Span(10, Seconds)) {
      df.queryExecution.executedPlan
    }
  }

  /**
   * We have additional behavior in PhsyicalOperation and ScanOperation to avoid excessive alias
   * subsitution when collapsing Projects.
   */
  test("SPARK-26626: Alias substitution does not explode plan in physical operation") {
    withTempPath { path =>
      // Write and read test data to make sure we cover the case in which ScanOperation
      // would otherwise collapse Projects and create outsized plans
      spark.range(10).toDF("col0")
        .write
        .save(path.getCanonicalPath)
      var df = spark.read.load(path.getCanonicalPath)

      // Create a nested query plan
      0 to 20 foreach { i =>
        df = df.withColumn(s"col${i + 1}", col(s"col$i") + col(s"col$i"))
      }

      // If ScanOperation collapsed projections and substituted aliases,
      // planning will take very long to complete
      cancelAfter(Span(20, Seconds)) {
        df.queryExecution.executedPlan
      }
    }

  }

  /**
   * We have additional config and logic to ignore file metadata when determining the calendar
   * rebase mode in `DataSourceUtils.datetimeRebaseMode`.
   */
  test("Force datetime rebase mode to derive from config instead of file metadata") {
    val path = getResourceParquetFilePath("test-data/date_with_ignorable_parquet_metadata.parquet")
    val metadata = getMetadata(path)

    // Sanity-check: We wrote a Parquet file with a single date '0001-01-01' and set the Spark
    // version in file metadata to 3.0.0. We didn't set the legacy datetime flag. Based on the
    // metadata, Spark will assume to use the 'corrected' calendar rebase mode.
    assert(metadata.toSet.contains(SPARK_VERSION_METADATA_KEY -> "3.0.0"))
    assertFalse(metadata.contains(SPARK_LEGACY_DATETIME))

    withSQLConf(
      SQLConf.LEGACY_PARQUET_REBASE_MODE_IN_READ.key -> "legacy",
      SQLConf.LEGACY_IGNORE_METADATA_FOR_REBASE_MODE_IN_READ.key -> "false") {
      // We set the rebase mode to 'legacy' so Spark should rebase datetimes into the new calendar
      // to get the correct value '0001-01-01'. But the metadata says the Spark version is 3.0 and
      // the legacy flag is not set. So Spark instead reads the file in 'corrected' mode, giving
      // the wrong value '0000-12-30'.
      checkAnswer(
        spark.read.parquet(path).select('date.cast("string")),
        Seq(Row("0000-12-30")))
    }

    withSQLConf(
      SQLConf.LEGACY_PARQUET_REBASE_MODE_IN_READ.key -> "legacy",
      SQLConf.LEGACY_IGNORE_METADATA_FOR_REBASE_MODE_IN_READ.key -> "true") {
      // Setting config to ignore file metadata means Spark will do exactly that and use the rebase
      // mode declared in Spark config, yielding the correct value.
      checkAnswer(
        spark.read.parquet(path).select('date.cast("string")),
        Seq(Row("0001-01-01")))
    }
  }

  def getMetadata(path: String): Map[String, String] = {
    import scala.collection.JavaConverters._

    val conf = new Configuration()
    val hadoopInputFile = HadoopInputFile.fromPath(new Path(path), conf)
    val parquetReadOptions = HadoopReadOptions.builder(conf).build()
    val reader = ParquetFileReader.open(hadoopInputFile, parquetReadOptions)
    val metadata = try {
      reader.getFileMetaData.getKeyValueMetaData
    } finally {
      reader.close()
    }
    metadata.asScala.toMap
  }
}
