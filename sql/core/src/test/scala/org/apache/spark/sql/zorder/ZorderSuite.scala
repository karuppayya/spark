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
import java.util.Comparator

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession

case class Data(intCol: Int, longCol1: Long,
                longCol2: Long = 0L, time: Long = 0L, random: Int = 0)

class ZorderComparator[T] extends Comparator[T] with Serializable {

  def less_msb(x: Long, y: Long): Boolean = {
    x < y && x < (x ^ y)
  }

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
    if (index == values1.length) {
      0
    } else if ((values1(index) - values2(index)) < 0) {
      -1
    } else {
      1
    }
  }

}

class ZorderSuite extends QueryTest with SharedSparkSession {
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

  test("z-order - Actual") {
    sql("CREATE TABLE a (id INT) USING PARQUET")
    val s: String = _
    import testImplicits._
    val r = scala.util.Random
    val from = LocalDate.of(2020, Month.JANUARY, 1)
    val to = LocalDate.of(2020, Month.DECEMBER, 1)

    val basePath = "s3://qubole-spar/karuppayya/SPAR-4460/benchmark/"
    val records = Seq(1000)
    records.foreach {
      records =>
        Seq(true, false).foreach {
          zorder =>
            val rdd = spark.range(records).map {
              id =>
                val date = LocalDate.ofEpochDay(from.toEpochDay +
                  r.nextInt((to.toEpochDay - from.toEpochDay) toInt))
                Data(id.toInt, id.toLong, id.toLong,
                  java.sql.Date.valueOf(date).getTime, r.nextInt())
            }.orderBy("random").toJavaRDD.zipWithIndex()
            val newRdd = if (zorder) {
              rdd.sortByKey(new ZorderComparator[Data]).keys()
            } else rdd.keys()
            val df = spark.createDataFrame(newRdd).as[Data]
            if (zorder) {
              df.write.parquet(basePath + s"$records/zorder")
              println(s"Write succesful record count = $records loc: ${basePath + s"$records/zorder"}")
            } else {
              df.write.parquet(basePath + s"$records/orig")
              println(s"Write succesful record count = $records loc: ${basePath + s"$records/orig"}")
            }
        }
    }
    val rdd = spark.range(1000).map {
      id =>
        val date = LocalDate.ofEpochDay(from.toEpochDay +
          r.nextInt((to.toEpochDay - from.toEpochDay) toInt))
        Data(id.toInt, id.toLong, id.toLong, java.sql.Date.valueOf(date).getTime, r.nextInt())
    }.orderBy("random")
      .toJavaRDD.zipWithIndex()
      .sortByKey(new ZorderComparator[Data])
      .keys()
    val df = spark.createDataFrame(rdd).as[Data]
    val results = df.collect()


    results
  }
}


