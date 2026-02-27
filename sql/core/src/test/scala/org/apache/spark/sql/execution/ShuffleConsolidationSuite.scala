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

package org.apache.spark.sql.execution

import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.shuffle.vault.ShuffleVaultManager
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanHelper, ShuffleQueryStageExec, SimpleCost, SimpleCostEvaluator}
import org.apache.spark.sql.execution.exchange.{ConsolidationCostEvaluator, ConsolidationShuffleExchangeExec, ReusedExchangeExec, ShuffleExchangeLike, ShuffleVaultExtensions}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Test suite for shuffle consolidation
 */
class ShuffleConsolidationSuite
  extends QueryTest
  with SharedSparkSession
  with AdaptiveSparkPlanHelper {

  private val remoteStoragePath = new Path(
    System.getProperty("java.io.tmpdir"), "shuffle-consolidation-test")

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.remote.storage.path",
        remoteStoragePath.toString + java.io.File.separator)
      .set("spark.shuffle.sort.io.plugin.class",
        "org.apache.spark.shuffle.sort.remote.HybridShuffleDataIO")
      .set("spark.sql.shuffle.consolidation.enabled", "true")
      .set("spark.sql.shuffle.consolidation.size.threshold", "0")
      .set("spark.shuffle.manager", classOf[ShuffleVaultManager].getName)
      .set("spark.plugins", classOf[
        org.apache.spark.shuffle.vault.ShuffleVaultPlugin].getName)
      .set("spark.sql.extensions", classOf[ShuffleVaultExtensions].getName)
      .set("spark.sql.adaptive.enabled", "true")
      .set(SQLConf.ADAPTIVE_CUSTOM_COST_EVALUATOR_CLASS.key,
        classOf[ConsolidationCostEvaluator].getName)
      .set("spark.sql.classic.shuffleDependency.fileCleanup.enabled", "false")
  }

  override def afterAll(): Unit = {
    try {
      // Clean up the remote storage directory
      val hadoopConf = SparkHadoopUtil.get.newConfiguration(spark.sparkContext.getConf)
      val fs = FileSystem.get(remoteStoragePath.toUri, hadoopConf)
      try {
        if (fs.exists(remoteStoragePath)) {
          fs.delete(remoteStoragePath, true)
        }
      } finally {
        fs.close()
      }
    } finally {
      super.afterAll()
    }
  }

  /**
   * Helper method to find consolidation shuffle stages from a plan.
   */
  private def findConsolidationShuffles(plan: SparkPlan): Seq[ShuffleQueryStageExec] = {
    collectWithSubqueries(plan) {
      case s: ShuffleQueryStageExec
        if s.shuffle.isInstanceOf[ConsolidationShuffleExchangeExec] => s
    }
  }

  /**
   * Helper method to find consolidation shuffle exchanges from a plan (non-adaptive mode).
   */
  private def findConsolidationExchanges(
      plan: SparkPlan): Seq[ConsolidationShuffleExchangeExec] = {
    collect(plan) {
      case e: ConsolidationShuffleExchangeExec => e
    }
  }

  /**
   * Verifies that a ConsolidationShuffleExchangeExec is inserted into the AQE plan when
   * shuffle consolidation is enabled. Runs a simple groupBy aggregation (which requires a
   * shuffle) and checks that exactly one consolidation stage appears in the executed plan.
   * The consolidation exchange re-shuffles data from the completed shuffle stage with
   * partition-ID pass-through semantics, writing the output to remote storage.
   */
  test("Check if ShuffleConsolidation ShuffleExchange is introduced, when enabled") {
    val df = spark.range(100)
      .selectExpr("id % 10 as key", "id as value")
      .groupBy("key")
      .count()
    df.collect()
    val plan = df.queryExecution.executedPlan

    // Check that consolidation shuffle stage was introduced
    val consolidationShuffles = findConsolidationShuffles(plan)

    assert(consolidationShuffles.size == 1,
      "Shuffle consolidation stage should be introduced when enabled")

    // Verify output correctness
    checkAnswer(df, (0 until 10).map(i => Row(i, 10L)))
  }

  /**
   * Verifies that the size threshold gate works: when
   * spark.sql.shuffle.consolidation.size.threshold is set to Long.MaxValue, no shuffle
   * stage's runtime size can exceed the threshold, so AddConsolidationShuffle skips
   * consolidation entirely. This ensures small shuffles are not unnecessarily re-shuffled
   * to remote storage.
   */
  test("shuffle consolidation respects size threshold") {
    withSQLConf(
      "spark.sql.shuffle.consolidation.size.threshold" -> Long.MaxValue.toString) {

      // With a very high threshold, consolidation should not be applied
      val df = spark.range(10)
        .selectExpr("id % 5 as key", "id as value")
        .groupBy("key")
        .count()

      df.collect()
      val plan = df.queryExecution.executedPlan

      // Check that consolidation shuffle stage was introduced
      val consolidationShuffles = findConsolidationShuffles(plan)

      assert(consolidationShuffles.isEmpty,
        "Shuffle consolidation should not be applied when size is below threshold")

      // Verify output correctness
      checkAnswer(df, (0 until 5).map(i => Row(i, 2L)))
    }
  }

  /**
   * Verifies that shuffle consolidation works in non-AQE (static) execution mode.
   * In this path, AddConsolidationShuffle is injected via ShuffleVaultExtensions using
   * injectColumnar (preColumnarTransitions), which runs inside
   * ApplyColumnarRulesAndInsertTransitions in QueryExecution.preparations. The rule
   * wraps each ShuffleExchangeExec with ENSURE_REQUIREMENTS origin in a
   * ConsolidationShuffleExchangeExec. Unlike AQE, there is no runtime size threshold
   * check because shuffle statistics are not available before execution.
   */
  test("shuffle consolidation in non-adaptive mode") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {

      val df = spark.range(100)
        .selectExpr("id % 10 as key", "id as value")
        .groupBy("key")
        .count()

      val plan = df.queryExecution.executedPlan

      // Check that consolidation shuffle stage was introduced
      val consolidationShuffles = findConsolidationExchanges(plan)

      assert(consolidationShuffles.size == 1,
        "Shuffle consolidation stage should be introduced when enabled")

      // Verify output correctness
      checkAnswer(df, (0 until 10).map(i => Row(i, 10L)))
    }
  }

  /**
   * Tests shuffle consolidation for aggregate operations.
   *
   * This test verifies the behavior for cases where a query stage maps to a partial
   * logical sub-tree. In aggregates, the shuffle exchange is in the middle of the
   * transformation (e.g., both parent HashAggregate and child ShuffleQueryStage point
   * to the same Aggregate logical node), so the consolidation uses the parent's logical
   * link to ensure the entire subtree is found together during re-planning.
   *
   * See [[org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
   * .replaceWithQueryStagesInLogicalPlan]] for details on how query stages
   * are mapped back to logical plans.
   */
  test("shuffle consolidation for aggregate operations") {
    withSQLConf(
      SQLConf.EXCHANGE_REUSE_ENABLED.key -> "false") {

      val df = spark.range(100)
        .selectExpr("id % 10 as key", "id as value")
        .groupBy("key")
        .count()
        .orderBy("key")

      df.collect()
      val plan = df.queryExecution.executedPlan

      val consolidationShuffles = findConsolidationShuffles(plan)

      // Should have consolidation stages for the aggregate shuffle
      assert(consolidationShuffles.nonEmpty,
        "Consolidation stages should be present for aggregate shuffles")

      // Verify output correctness
      checkAnswer(df, (0 until 10).map(i => Row(i, 10L)))
    }
  }

  /**
   * Tests shuffle consolidation for join operations.
   *
   * This test verifies the behavior for cases where a query stage maps to an integral
   * logical sub-tree. In joins, each side of the join (left and right) forms a complete
   * relation, so the query stage and parent typically point to different logical nodes.
   * The consolidation uses the stage's logical link to maintain the proper mapping.
   *
   * See [[org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
   * .replaceWithQueryStagesInLogicalPlan]] for details on how query stages
   * are mapped back to logical plans.
   */
  test("shuffle consolidation for join operations") {
    withSQLConf(
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {

      val df1 = spark.range(50)
        .selectExpr("id as key", "id * 2 as value1")
      val df2 = spark.range(50)
        .selectExpr("id as key", "id * 3 as value2")

      val joined = df1.join(df2, "key").orderBy("key")

      joined.collect()
      val plan = joined.queryExecution.executedPlan

      val consolidationShuffles = findConsolidationShuffles(plan)

      assert(consolidationShuffles.nonEmpty,
        "Shuffle consolidation stages should be introduced for join operations")

      // Verify output correctness
      checkAnswer(joined,
        (0 until 50).map(i => Row(i, i * 2, i * 3)))
    }
  }

  /**
   * Verifies that stock Spark's exchange reuse mechanism (ReuseExchangeAndSubquery) continues
   * to work for regular ShuffleExchangeExec nodes when shuffle consolidation is enabled.
   * A self-join produces two identical shuffle exchanges; the rule deduplicates them by
   * replacing the second with a ReusedExchangeExec that points to the first. This test
   * runs in non-AQE mode so that ReuseExchangeAndSubquery operates directly on the plan.
   */
  test("exchange reuse works for non-consolidation shuffles when consolidation is enabled") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.EXCHANGE_REUSE_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {

      val df = spark.range(100)
        .selectExpr("id % 10 as key", "id as value")

      // Self-join creates two identical shuffle exchanges that can be reused
      val joined = df.join(df, "key")

      val plan = joined.queryExecution.executedPlan
      val reusedExchanges = collect(plan) {
        case r: ReusedExchangeExec => r
      }

      assert(reusedExchanges.nonEmpty,
        "Regular shuffle exchanges should be reused even when consolidation is enabled")
    }
  }

  /**
   * Verifies that ConsolidationShuffleExchangeExec nodes are eligible for exchange reuse
   * in non-AQE mode. In a self-join, both sides of the join produce an identical shuffle
   * followed by an identical consolidation exchange. ReuseExchangeAndSubquery detects
   * that the two consolidation exchanges have the same canonicalized form and replaces
   * the duplicate with a ReusedExchangeExec. This is correct because both consolidation
   * exchanges read from the same upstream shuffle data and would produce identical output;
   * reusing avoids a redundant re-shuffle and duplicate write to remote storage.
   */
  test("consolidation exchange reuse in non-adaptive mode") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.EXCHANGE_REUSE_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {

      val df = spark.range(10)
        .selectExpr("id as key", "id as value")

      // Self-join: both sides produce identical shuffle + consolidation exchanges
      val joined = df.join(df, "key")

      val plan = joined.queryExecution.executedPlan

      // Consolidation exchanges should be present
      val consolidationExchanges = findConsolidationExchanges(plan)
      assert(consolidationExchanges.nonEmpty,
        "Consolidation exchanges should be present in non-AQE mode")

      // At least one consolidation exchange should be reused (the duplicate from self-join)
      val reusedConsolidation = collect(plan) {
        case r: ReusedExchangeExec
          if r.child.isInstanceOf[ConsolidationShuffleExchangeExec] => r
      }
      assert(reusedConsolidation.nonEmpty,
        "Identical consolidation exchanges should be reused in non-AQE mode")

      // Verify output correctness (1:1 join, 10 rows)
      checkAnswer(joined,
        (0 until 10).map(i => Row(i, i, i)))
    }
  }

  /**
   * Verifies that consolidation exchanges work correctly with AQE's exchange reuse.
   * In AQE, exchanges are wrapped in ShuffleQueryStageExec (leaf nodes), so
   * ReuseExchangeAndSubquery does not apply. Instead, AQE has its own reuse mechanism
   * that deduplicates query stages with identical canonicalized plans. This test ensures
   * that a self-join with consolidation produces correct results regardless of whether
   * AQE reuses the underlying shuffle stages.
   */
  test("consolidation exchange reuse in adaptive mode") {
    withSQLConf(
      SQLConf.EXCHANGE_REUSE_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {

      val df = spark.range(10)
        .selectExpr("id as key", "id as value")

      // Self-join: both sides produce identical shuffles
      val joined = df.join(df, "key")

      joined.collect()
      val plan = joined.queryExecution.executedPlan

      // Consolidation stages should be present
      val consolidationShuffles = findConsolidationShuffles(plan)
      assert(consolidationShuffles.nonEmpty,
        "Consolidation stages should be present in AQE mode")

      // Verify output correctness (1:1 join, 10 rows)
      checkAnswer(joined,
        (0 until 10).map(i => Row(i, i, i)))
    }
  }

  /**
   * Verifies that ConsolidationCostEvaluator correctly excludes ConsolidationShuffleExchangeExec
   * from the shuffle count when evaluating plan cost. This is critical for AQE plan comparison:
   * without this, AQE would penalize plans that include consolidation exchanges (treating them
   * as extra shuffles), potentially choosing an inferior plan without consolidation. The test
   * runs in non-AQE mode so the executed plan contains raw exchange nodes (not wrapped in
   * ShuffleQueryStageExec), then compares the cost from ConsolidationCostEvaluator against
   * SimpleCostEvaluator. The custom evaluator should return a strictly lower cost because
   * it does not count consolidation exchanges.
   */
  test("ConsolidationCostEvaluator excludes consolidation exchanges from cost") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val df = spark.range(100)
        .selectExpr("id % 10 as key", "id as value")
        .groupBy("key")
        .count()
      val plan = df.queryExecution.executedPlan

      // Verify both regular and consolidation exchanges are present
      val allExchanges = collect(plan) { case s: ShuffleExchangeLike => s }
      val consolidationExchanges = findConsolidationExchanges(plan)
      assert(consolidationExchanges.nonEmpty, "Consolidation exchanges should be present")
      assert(allExchanges.size > consolidationExchanges.size,
        "Plan should have both regular and consolidation exchanges")

      // ConsolidationCostEvaluator should exclude consolidation exchanges
      val consolidationCost = new ConsolidationCostEvaluator().evaluateCost(plan)
      // SimpleCostEvaluator counts all exchanges including consolidation
      val simpleCost = SimpleCostEvaluator(forceOptimizeSkewedJoin = false).evaluateCost(plan)

      assert(consolidationCost.asInstanceOf[SimpleCost].value <
        simpleCost.asInstanceOf[SimpleCost].value,
        "ConsolidationCostEvaluator should produce lower cost by excluding consolidation exchanges")

      // Verify output correctness
      checkAnswer(df, (0 until 10).map(i => Row(i, 10L)))
    }
  }

  /**
   * End-to-end test that verifies shuffle consolidation actually writes data files to the
   * configured remote storage path. Runs a groupBy aggregation with consolidation enabled,
   * then inspects the Hadoop filesystem at the remote storage path to confirm that
   * shuffle data files (files starting with "shuffle_" and not containing "checksum") exist
   * and have non-zero size. This validates the full write path: ShuffleVaultManager routes
   * the consolidation shuffle to RemoteSortShuffleWriter (via ShuffleVaultHandle),
   * HybridShuffleExecutorComponents delegates to RemoteShuffleMapOutputWriter, and the
   * output lands at RemoteShuffleStorage.getPath(blockId).
   */
  test("shuffle consolidation writes data to remote storage") {
    val df = spark.range(1000)
      .selectExpr("id % 100 as key", "id as value")
      .groupBy("key")
      .count()

    df.collect()

    // Get the Hadoop FileSystem
    val hadoopConf = SparkHadoopUtil.get.newConfiguration(spark.sparkContext.getConf)
    val fs = FileSystem.get(remoteStoragePath.toUri, hadoopConf)
    try {
      // Check if the remote path exists
      assert(fs.exists(remoteStoragePath),
        s"Remote shuffle path does not exist: $remoteStoragePath")

      // Look for shuffle data files recursively
      var foundDataFiles = false
      var totalSize = 0L

      val dataFiles = fs.listFiles(remoteStoragePath, true)
      while (dataFiles.hasNext) {
        val file = dataFiles.next()
        if (file.getPath.getName.startsWith("shuffle_") &&
            !file.getPath.getName.contains("checksum")) {
          foundDataFiles = true
          totalSize += file.getLen
        }
      }

      assert(foundDataFiles,
        "No shuffle data files found in remote storage")
      assert(totalSize > 0,
        s"Shuffle data files exist but contain no data (total size: $totalSize)")
    } finally {
      fs.close()
    }

    // Verify output correctness
    checkAnswer(df, (0 until 100).map(i => Row(i, 10L)))
  }
}
