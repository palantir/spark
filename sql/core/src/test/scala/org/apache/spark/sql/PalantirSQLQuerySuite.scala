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

import org.scalatest.concurrent.{Signaler, ThreadSignaler, TimeLimits}
import org.scalatest.time.{Seconds, Span}

import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Additional tests to cover Palantir-specific additions.
 */
class PalantirSQLQuerySuite extends QueryTest
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
}
