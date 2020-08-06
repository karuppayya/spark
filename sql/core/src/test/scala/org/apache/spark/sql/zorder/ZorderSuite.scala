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

import java.time.{LocalDate, Month}
import java.util
import java.util.Comparator
import java.util.concurrent.TimeUnit.NANOSECONDS

import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SQLTestData.TestData2
import org.apache.spark.sql.test.{SQLTestUtils, SharedSparkSession}
import org.apache.spark.sql.QueryTest

case class Data(intCol: Long, longCol1: Long,
                longCol2: Long = 0, time: Long = 0L, random: Int = 0)
case class Metric(numRecords: Long, isZordered: Boolean, orderedColCount: Int,
                  timeTakenInMs: Long, pred: String, predCount: Int)

class ZorderComparator[T] extends Comparator[T] with Serializable {

  def compare(o11: T, o22: T): Int = {
    val o1 = o11.asInstanceOf[Data]
    val o2 = o22.asInstanceOf[Data]
    val values1: Array[Long] = Array(o1.intCol, o1.longCol1, o1.longCol2)
    val values2: Array[Long] = Array(o2.intCol, o2.longCol1, o2.longCol2)
    var index = 0
    while (index < values1.length - 1 && less_msb(values1(index) ^ values2(index),
      values1(index + 1) ^ values2(index + 1))) {
      index = index + 1
    }
    if ((values1(index) - values2(index)) == 0) {
      0
    } else if ((values1(index) - values2(index)) > 0) {
      1
    } else {
      -1
    }
  }

  def less_msb(x: Long, y: Long): Boolean = {
    x < y && x < (x ^ y)
  }

}

class ZorderSuite extends QueryTest
  with SharedSparkSession with SQLTestUtils {

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    loadTestData()
  }

  test("Range partitioning1") {
    val arr = (1 to 10).flatMap {
      x =>
        (1 to 10).flatMap {
          y =>
            (1 to 10).map {
              z => Data(x, y, z)
            }
        }
    }
    util.Arrays.sort(arr.toArray, new ZorderComparator[Data])
  }

  test("Range partitioning") {
    val loc = "file:///tmp/zorder/5"
    val df = sql("select * from testdata2 order by a, b")
    df.repartition(1)
      .write
      .mode("overwrite")
      .csv(loc)
  }

  test("z-order - Golden - Naive") {

    import testImplicits._


    val data = (0 to 7).flatMap {
      x =>
        (0 to 7).map {
          y => Data(x, y)
        }
    }
    val dsRdd = data.toDS()
      .repartition(1)
      .toJavaRDD.zipWithIndex()
      .sortByKey(new ZorderComparator[Data])
      .keys()
    val newDS = spark.createDataFrame(dsRdd).as[Data]
    val results = newDS.collect()
    results
  }

  test("without z order") {
    def less_msb(x: Long, y: Long): Boolean = {
      x < y && x < (x ^ y)
    }

    val data = (0 to 7).flatMap {
      x =>
        (0 to 7).map {
          y => TestData2(x, y)
        }
    }

    val sorted = data.sortWith {
      case (data1, data2) =>
        val values1: Array[Long] = Array(data1.a, data1.b)
        val values2: Array[Long] = Array(data2.a, data2.b)
        var index = 0
        while (index < values1.length - 1 && less_msb(values1(index) ^ values2(index),
          values1(index + 1) ^ values2(index + 1))) {
          index = index + 1
        }
        (values1(index) - values2(index)) < 0
    }

    import testImplicits._
    sorted.toDS().repartition(1).write.csv("/tmp/golden")
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
                sql(s"set ${SQLConf.ZORDER_ENABLED.key}=$zorder").collect()
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
                  df.orderBy(cols.head, cols.drop(1): _*)
                    .write.mode("overwrite").parquet(s"$basePath/$num/zorder/$key")
                  println(s"Write succesful record count = $num loc: ${s"$basePath/$num/zorder/$key"}")
                } else {
                  df.orderBy("random")
                    .write.mode("overwrite").parquet(s"$basePath/$num/orig/$key")
                  println(s"Write succesful record count = $num loc: ${s"$basePath/$num/orig/$key"}")
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
}


