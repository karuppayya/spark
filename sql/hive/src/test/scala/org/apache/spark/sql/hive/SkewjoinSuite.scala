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

package org.apache.spark.sql.hive

import org.apache.spark.{MapOutputTrackerMaster, SparkConf, SparkContext, SparkEnv}
import org.apache.spark.internal.config
import org.apache.spark.internal.config.UI.UI_ENABLED
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.optimizer.ConvertToLocalRelation
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.hive.test.{TestHiveContext, TestHiveSparkSession}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SQLTestUtils

case class Employee(id: Int, name: String)

/*
 * Tests for skew detection
 */
class SkewjoinSuite extends SQLTestUtils {

  val spark: SparkSession = {
    val conf: SparkConf = new SparkConf()
    conf
      .set("spark.sql.test", "")
      .set(SQLConf.CODEGEN_FALLBACK.key, "false")
      .set("spark.sql.warehouse.dir", TestHiveContext.makeWarehouseDir().toURI.getPath)
      .set(UI_ENABLED, false)
      .set(config.UNSAFE_EXCEPTION_ON_MEMORY_LEAK, true)
      .set("spark.hadoop.hive.metastore.disallow.incompatible.col.type.changes", "false")
      .set(SQLConf.OPTIMIZER_EXCLUDED_RULES.key, ConvertToLocalRelation.ruleName)
      .set("spark.skew.handling.enabled", "true")
      .setMaster("local")
      .setAppName("Skew tests")
    val sc: SparkContext = new SparkContext(conf)
    new TestHiveSparkSession(HiveUtils.withHiveExternalCatalog(sc), true)
  }

  test("size estimation for relations is based on row size * number of rows") {
    val skewTable = "skewtable"
    val normalTable = "tbl"

    import spark.implicits._
    withTable(skewTable, normalTable) {
      val df = spark.range(1000).map {
        x =>
          if (x < 100) {
            Employee(1, s"name$x")
          } else {
            Employee(x.toInt, s"name$x")
          }
      }
      df.write.saveAsTable(skewTable)
      spark.range(1000)
        .write.saveAsTable(normalTable)
      withSQLConf("spark.sql.autoBroadcastJoinThreshold" -> "-1",
        "spark.sql.shuffle.partitions"-> "200") {
        val joinDF = spark.sql(s"select * from $skewTable a join $normalTable b ON a.id = b.id")
        joinDF.collect()

          val shuffleId = joinDF.queryExecution.executedPlan.collect {
          case s: ShuffleExchangeExec =>
            val leaves = s.collectLeaves()
            val leaf = leaves.head
            if (leaf.isInstanceOf[FileSourceScanExec] &&
                leaf.asInstanceOf[FileSourceScanExec].tableIdentifier.get.table == skewTable) {
              Some(s.shuffleDependency)
            } else {
              None
            }
        }.filter(_.isDefined).map(_.get)

        assert(shuffleId.size == 1, "Incorrect number of shuffle id")
        val stats = SparkEnv.get.mapOutputTracker
          .asInstanceOf[MapOutputTrackerMaster]
          .getStatistics(shuffleId.head)
        assert(stats.skewedKeyValue.isDefined)
        assert(stats.skewedKeyValue.get._2 == 100)
      }
    }
  }
}
