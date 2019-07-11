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

import org.apache.spark.  {MapOutputTrackerMaster, SparkConf, SparkContext, SparkEnv}
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

  val skewTable = "skewtable"
  val normalTable1 = "tbl1"
  val normalTable2 = "tbl2"

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

  override def beforeAll(): Unit = {
    super.beforeAll()
    createTables()
  }

  def createTables(): Unit = {

    import spark.implicits._
    spark.range(1000).map {
      x =>
        if (x < 100) {
          Employee(1, s"name$x")
        } else {
          Employee(x.toInt, s"name$x")
        }
    }.write.saveAsTable(skewTable)
    spark.range(1000)
      .write.saveAsTable(normalTable1)
    spark.range(1000).map(x => Employee(x.toInt, s"name$x"))
      .write.saveAsTable(normalTable2)
  }

  test("size estimation for relations is based on row size * number of rows") {

    withSQLConf("spark.sql.autoBroadcastJoinThreshold" -> "-1",
      "spark.sql.shuffle.partitions" -> "200") {
      val joinDF = spark.sql(s"select * from $skewTable a join $normalTable1 b ON a.id = b.id")
      joinDF.collect()

      val shuffleId = joinDF.queryExecution.executedPlan.collect {
        case s: ShuffleExchangeExec =>
          val leaves = s.collectLeaves()
          val leaf = leaves.head
          leaf match {
            case l: FileSourceScanExec if l.tableIdentifier.get.table == skewTable =>
              Some(s.shuffleDependency)
            case _ =>
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

  test("size estimation for relations is based on row size * number of rows1") {

    withSQLConf("spark.sql.autoBroadcastJoinThreshold" -> "-1",
      "spark.sql.shuffle.partitions" -> "200") {
      val joinDF = spark.sql(s"select * from $normalTable1 a join $normalTable2 b ON a.id = b.id")
      joinDF.collect()

      val shuffleId = joinDF.queryExecution.executedPlan.collect {
        case s: ShuffleExchangeExec =>
          val leaves = s.collectLeaves()
          val leaf = leaves.head
          leaf match {
            case l: FileSourceScanExec =>
              Some(s.shuffleDependency)
            case _ =>
              None
          }
      }.filter(_.isDefined).map(_.get)

      assert(shuffleId.size == 2, "Incorrect number of shuffle id")
      val statsEmpty = shuffleId.map {
        id => SparkEnv.get.mapOutputTracker
          .asInstanceOf[MapOutputTrackerMaster]
          .getStatistics(id)
      }.forall(_.skewedKeyValue.isEmpty)
      assert(statsEmpty   )
    }
  }

}
