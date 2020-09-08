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

package org.apache.spark.sql.zorder

import java.io.File

import org.apache.spark.sql.{AnalysisException, QueryTest}
import org.apache.spark.sql.test.{SharedSparkSession, SQLTestUtils}
import org.apache.spark.sql.test.SQLTestData.ZorderData
import org.apache.spark.util.Utils

case class Data(intCol: Long, longCol1: Long,
                longCol2: Long = 0, time: Long = 0L, random: Int = 0)
case class Metric(numRecords: Long, isZordered: Boolean, orderedColCount: Int,
                  timeTakenInMs: Long, pred: String, predCount: Int)

class ZorderSuite extends QueryTest
  with SharedSparkSession with SQLTestUtils {

  var dir: File = _
  val tblName = "ztbl"

  val golden: Seq[ZorderData] = {
    Seq(ZorderData(0, 0),
      ZorderData(0, 1),
      ZorderData(1, 0),
      ZorderData(1, 1),
      ZorderData(0, 2),
      ZorderData(0, 3),
      ZorderData(1, 2),
      ZorderData(1, 3),
      ZorderData(2, 0),
      ZorderData(2, 1),
      ZorderData(3, 0),
      ZorderData(3, 1),
      ZorderData(2, 2),
      ZorderData(2, 3),
      ZorderData(3, 2),
      ZorderData(3, 3),
      ZorderData(0, 4),
      ZorderData(0, 5),
      ZorderData(1, 4),
      ZorderData(1, 5),
      ZorderData(0, 6),
      ZorderData(1, 6),
      ZorderData(2, 4),
      ZorderData(2, 5),
      ZorderData(3, 4),
      ZorderData(3, 5),
      ZorderData(2, 6),
      ZorderData(3, 6),
      ZorderData(4, 0),
      ZorderData(4, 1),
      ZorderData(5, 0),
      ZorderData(5, 1),
      ZorderData(4, 2),
      ZorderData(4, 3),
      ZorderData(5, 2),
      ZorderData(5, 3),
      ZorderData(6, 0),
      ZorderData(6, 1),
      ZorderData(6, 2),
      ZorderData(6, 3),
      ZorderData(4, 4),
      ZorderData(4, 5),
      ZorderData(5, 4),
      ZorderData(5, 5),
      ZorderData(4, 6),
      ZorderData(5, 6),
      ZorderData(6, 4),
      ZorderData(6, 5),
      ZorderData(6, 6))
  }

  val ngolden: Seq[ZorderData] = {
    Seq(ZorderData(0, 0),
    ZorderData(0, -6),
    ZorderData(0, -5),
    ZorderData(0, -4),
    ZorderData(0, -3),
    ZorderData(0, -2),
    ZorderData(0, -1),
    ZorderData(-6, 0),
    ZorderData(-5, 0),
    ZorderData(-4, 0),
    ZorderData(-3, 0),
    ZorderData(-2, 0),
    ZorderData(-1, 0),
    ZorderData(-6, -6),
    ZorderData(-6, -5),
    ZorderData(-5, -6),
    ZorderData(-5, -5),
    ZorderData(-6, -4),
    ZorderData(-6, -3),
    ZorderData(-5, -4),
    ZorderData(-5, -3),
    ZorderData(-6, -2),
    ZorderData(-6, -1),
    ZorderData(-5, -2),
    ZorderData(-5, -1),
    ZorderData(-4, -6),
    ZorderData(-4, -5),
    ZorderData(-3, -6),
    ZorderData(-3, -5),
    ZorderData(-2, -6),
    ZorderData(-2, -5),
    ZorderData(-1, -6),
    ZorderData(-1, -5),
    ZorderData(-4, -4),
    ZorderData(-4, -3),
    ZorderData(-3, -4),
    ZorderData(-3, -3),
    ZorderData(-4, -2),
    ZorderData(-4, -1),
    ZorderData(-3, -2),
    ZorderData(-3, -1),
    ZorderData(-2, -4),
    ZorderData(-2, -3),
    ZorderData(-1, -4),
    ZorderData(-1, -3),
    ZorderData(-2, -2),
    ZorderData(-2, -1),
    ZorderData(-1, -2),
    ZorderData(-1, -1))
  }

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    sql(s"DROP TABLE IF EXISTS $tblName").collect()
    dir = Utils.createTempDir()
    loadTestData()
  }

  test("Z-order: writes - SQL") {
    import testImplicits._
    sql("SET spark.sql.shuffle.partitions = 1")
    // CTAS
    val query =
      s"""
        | create table a
        | USING PARQUET
        | LOCATION "file:///tmp/data/zorder"
        | AS
        | SELECT * FROM zorderdata ZORDER BY a, b
        |""".stripMargin
    sql(query).collect()
    val results = spark.read.parquet("file:///tmp/data/zorder").as[ZorderData].collect()
    assert(results.toSeq == golden)

    // Insert overwrite
    // Create table
    sql(s"CREATE TABLE $tblName (a INT, b INT, c STRING) USING PARQUET")
    sql(s"INSERT OVERWRITE $tblName SELECT * FROM zorderdata ZORDER BY a, b")
    spark.table(tblName).as[ZorderData].collect()
    assert(spark.table(tblName).as[ZorderData].collect().toSeq == golden)
  }

  test("Z-order: writes - Dataframe api") {
    import testImplicits._
    sql("SET spark.sql.shuffle.partitions = 1")

    spark.table("zorderdata")
      .write
      .mode("overwrite")
      .zorderBy("a", "b").save("/tmp/data")
    val res = spark.read.parquet("file:///tmp/data").as[ZorderData].collect()
    assert(res.toSeq == golden)

    spark.table("zorderdata")
      .write
      .mode("overwrite")
      .zorderBy("a", "b").saveAsTable("zTable")
    val res1 = spark.table("zTable").as[ZorderData].collect()
    assert(res1.toSeq == golden)

  }

  test("Negative cases") {
    val bucketException = intercept[AnalysisException] {
      val df = spark.table("zorderdata")
      df.write.bucketBy(1, "a").zorderBy("a", "b").save()
    }
    assert(bucketException.getMessage
      .contains("Cannot Zorder, table contains bucketing properties"))

    val formatException = intercept[AnalysisException] {
      val df = spark.table("zorderdata")
      df.write.format("csv").zorderBy("a", "b").save()
    }
    assert(formatException.getMessage.contains(s"Data can be Z-ordered only on Parquet or Orc" +
      s"format"))

    val repartException = intercept[AnalysisException] {
      val df = spark.table("zorderdata")
      df.repartition(1).write.zorderBy("a", "b").save()
    }
    assert(repartException.getMessage.contains("Zorder will affect the number of partitions"))
  }

  test("Z-order:Read - Dataframe API") {
    import testImplicits._
    val df = spark.table("zorderdata")
      .write
      .zorderBy("a", "b")
    df.mode("overwrite").save("file:///tmp/zorder")
    val ress = spark.read.parquet("file:///tmp/zorder").as[ZorderData].collect()
    assert(ress.toSeq == golden)
  }

  test("Z-order:Read - SQL") {
    val df = spark.sql("select * from zorderdata zorder by a, b")
    val res = df.collect().map(r => ZorderData(r.getInt(0), r.getInt(1)) )
    assert(res.toSeq == golden)
  }

  test("datatype validations") {
    intercept[AnalysisException] {
      val df = spark.sql("select * from zorderdata zorder by a, c")
      df.collect()
    }

    intercept[AnalysisException] {
      val df = spark.table("zorderdata")
      df.zorderBy("a", "c")
    }

  }

  test("max") {
    val res = sql("select max(a), max(b) from testdata2").collect()
    res
  }
  test("Z order - init") {

    val d: Seq[(ZorderData, ZorderData)] = (0 to 6).flatMap {
      x =>
        (0 to 6).map {
          y => (ZorderData(x, y), ZorderData(-x, -y))
        }
    }
    val data = d.map(_._1)
    val data1 = d.map(_._2)

    val testCases: Seq[(Seq[ZorderData], Seq[ZorderData])] = Seq(
      (data, golden),
      (data1, ngolden))

    def less_msb(x: Long, y: Long): Boolean = {
      java.lang.Long.compareUnsigned(x, y) < 0 && java.lang.Long.compareUnsigned(x, x ^ y) < 0
    }

    testCases.foreach {
      case (data, golden) =>
        val sorted = data.sortWith {
          case (data1, data2) =>
            val values1: Array[Long] = Array(data1.a ^ Long.MinValue, data1.b ^ Long.MinValue)
            val values2: Array[Long] = Array(data2.a ^ Long.MinValue, data2.b ^ Long.MinValue)
            var msd = 0
            for (dim <- 1 until values1.length) {
              val l1 = values1(msd) ^ values2(msd)
              val l2 = values1(dim) ^ values2(dim)
              if (less_msb(l1, l2)) {
                msd = dim
              }
            }
            values1(msd) < values2(msd)
        }

        import testImplicits._
        val results = data.toDS()
          .repartition(1).zorderBy("a", "b").collect()
        assert(sorted == golden)
        assert(results.toSeq == golden)
    }

  }

  protected override def afterAll(): Unit = {
    Utils.deleteRecursively(dir)
    super.afterAll()
  }
}


