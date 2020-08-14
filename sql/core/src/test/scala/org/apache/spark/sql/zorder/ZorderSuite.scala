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
import java.time.{LocalDate, Month}
import java.util.concurrent.TimeUnit.NANOSECONDS

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.{SharedSparkSession, SQLTestUtils}
import org.apache.spark.sql.test.SQLTestData.TestData2
import org.apache.spark.util.Utils

case class Data(intCol: Long, longCol1: Long,
                longCol2: Long = 0, time: Long = 0L, random: Int = 0)
case class Metric(numRecords: Long, isZordered: Boolean, orderedColCount: Int,
                  timeTakenInMs: Long, pred: String, predCount: Int)

class ZorderSuite extends QueryTest
  with SharedSparkSession with SQLTestUtils {

  var dir: File = _
  protected override def beforeAll(): Unit = {
    super.beforeAll()
    dir = Utils.createTempDir()
    loadTestData()
  }

  test("Dataframe API") {
    import testImplicits._
    val df = spark.table("testdata2")
      .repartition(1)
      .write
      .zorderBy("a", "b")
    df.mode("overwrite").save("file:///tmp/zorder")
    val ress = spark.read.parquet("file:///tmp/zorder").as[TestData2].collect()
    assert(ress.toSeq == golden)
  }

  test("SQL") {
    val df = spark.sql("select * from testdata2 zorder by a, b")
    val res = df.collect().map(r => TestData2(r.getInt(0), r.getInt(1)) )
    assert(res.toSeq == golden)
  }

  test("Z order - init") {
    def less_msb(x: Long, y: Long): Boolean = {
      x < y && x < (x ^ y)
    }

    val data = (0 to 6).flatMap {
      x =>
        (0 to 6).map {
          y => TestData2(x, y)
        }
    }

    val sorted = data.sortWith {
      case (data1, data2) =>
        val values1: Array[Long] = Array(data1.a, data1.b)
        val values2: Array[Long] = Array(data2.a, data2.b)
        var msd = 0
        for (dim <- 1 until values1.length) {
          val l1 = values1(msd) ^ values2(msd)
          val l2 = values1(dim) ^ values2(dim)
          if (less_msb(l1, l2)) {
            msd = dim;
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

  test("z-order - Actual") {

    val spark = org.apache.spark.sql.SparkSession.getActiveSession.get

    case class Query(pred: String, numPreds: Int)
    val queriesMap =
      Seq(100 -> Seq(
        // BOUNDED QUERY
        // selectivity 10%
        Query(s"select * from a where intcol > 500 and intcol < 100500", 1),
        Query(s"select * from a where intcol > 500 and intcol < 100500 and longCol1 > 1000 and longCol1 < 100100", 2),
        Query(s"select * from a where intcol > 500 and intcol < 100500 and longCol1 > 1000 and longCol1 < 100100 and longCol2 > 1000 and longCol2 < 100100", 3)),
        1000 -> Seq(
          // BOUNDED QUERY
          // selectivity 10%
          Query(s"select * from a where intcol > 500 and intcol < 1005000", 1),
          Query(s"select * from a where intcol > 500 and intcol < 1005000 and longCol1 > 1000 and longCol1 < 1001000", 2),
          Query(s"select * from a where intcol > 500 and intcol < 1005000 and longCol1 > 1000 and longCol1 < 1001000 and longCol2 > 1000 and longCol2 < 1001000", 3))).toMap
    val locs = Seq("zorder", "orig")

    val r = scala.util.Random
    val from = LocalDate.of(2020, Month.JANUARY, 1)
    val to = LocalDate.of(2020, Month.DECEMBER, 1)

    val basePath = "s3://qubole-spar/karuppayya/SPAR-4460/benchmark"
    val records = Seq(100, 1000)

    val orderingCols = Map(1 -> Seq("intcol"),
      2 -> Seq("intcol", "longcol1"),
      3 -> Seq("intcol", "longcol1", "longcol2"))

    import spark.implicits._
    records.foreach {
      num =>
        Seq(true, false).foreach {
          zorder =>
            val df = orderingCols.foreach {
              case (key, cols) =>
                val df1 = spark.range(num).repartition(200)
                val df2 = spark.range(num).repartition(200)
                val df3 = spark.range(num).repartition(200)
                val newDF = df1.join(df2).join(df3)
                val df = newDF.map {
                  row =>
                    val x = row.getLong(0)
                    val y = row.getLong(1)
                    val z = row.getLong(2)
                    Data(x, y, z, r.nextLong(), r.nextInt())

                }
                if (zorder) {
                  if (cols.size > 1) {
                    df.zorderBy(cols(0), cols(1), cols.drop(2): _*)
                      .write.mode("overwrite").parquet(s"$basePath/$num/zorder/$key")
                    // println(s"Write succesful record count
                    // = $num loc: ${s"$basePath/$num/zorder/$key"}")
                  }
                } else {
                  df.orderBy("random")
                    .write.mode("overwrite").parquet(s"$basePath/$num/orig/$key")
                  // println(s"Write succesful record count
                  // = $num loc: ${s"$basePath/$num/orig/$key"}")
                }
            }
        }
    }


    val time = records.flatMap {
      numRecords =>
        locs.flatMap {
          location =>
            orderingCols.flatMap {
              case (key, _) =>
                spark.read
                  .parquet(s"$basePath/$numRecords/$location/$key").createOrReplaceTempView("a")
                val queries = queriesMap(numRecords)
                queries.map {
                  query =>
                    val start = System.nanoTime()
                    val df = sql(query.pred)
                    df.write.mode("overwrite").parquet(basePath + "/results")
                    val end = System.nanoTime()
                    val time = NANOSECONDS.toMillis(end - start)
                    Metric(numRecords, location == "zorder", key, time, query.pred, query.numPreds)
                }
            }
        }
    }
    val newDF = time.toDS().groupBy("pred", "numRecords", "predCount", "orderedColCount")
      .pivot("isZordered")
      .sum("timeTakenInMs")
      .orderBy("numRecords", "predCount", "orderedColCount")

    newDF.show(100)
  }

  val golden = {
    Seq(TestData2(0, 0),
      TestData2(0, 1),
      TestData2(1, 0),
      TestData2(1, 1),
      TestData2(0, 2),
      TestData2(0, 3),
      TestData2(1, 2),
      TestData2(1, 3),
      TestData2(2, 0),
      TestData2(2, 1),
      TestData2(3, 0),
      TestData2(3, 1),
      TestData2(2, 2),
      TestData2(2, 3),
      TestData2(3, 2),
      TestData2(3, 3),
      TestData2(0, 4),
      TestData2(0, 5),
      TestData2(1, 4),
      TestData2(1, 5),
      TestData2(0, 6),
      TestData2(1, 6),
      TestData2(2, 4),
      TestData2(2, 5),
      TestData2(3, 4),
      TestData2(3, 5),
      TestData2(2, 6),
      TestData2(3, 6),
      TestData2(4, 0),
      TestData2(4, 1),
      TestData2(5, 0),
      TestData2(5, 1),
      TestData2(4, 2),
      TestData2(4, 3),
      TestData2(5, 2),
      TestData2(5, 3),
      TestData2(6, 0),
      TestData2(6, 1),
      TestData2(6, 2),
      TestData2(6, 3),
      TestData2(4, 4),
      TestData2(4, 5),
      TestData2(5, 4),
      TestData2(5, 5),
      TestData2(4, 6),
      TestData2(5, 6),
      TestData2(6, 4),
      TestData2(6, 5),
      TestData2(6, 6))
  }

  protected override def afterAll(): Unit = {
    Utils.deleteRecursively(dir)
    super.afterAll()
  }
}


