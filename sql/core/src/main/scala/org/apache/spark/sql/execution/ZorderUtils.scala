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

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.types.{DataType, IntegerType, LongType, TimestampType}

object ZorderUtils {

  def canApplyZordering(types: Seq[DataType]): Boolean = {
    types.forall(dType =>
      TypeUtils.checkForNumericExpr(dType, "zorder").isSuccess)
  }

  def ZorderDF(zorderedColumns: String,
    child: SparkPlan): Dataset[Row] = {
    val sparkSession = SparkSession.getActiveSession.get
    val zCols = zorderedColumns.split(",")
    // Execute the RDD and persist
    val rdd = child.execute().mapPartitionsInternal {
      iter =>
        iter.map(_.copy())
    }
    rdd.persist()
    // Create a dataframe from RDD and aggregate min/max stats
    val df = sparkSession.internalCreateDataFrame(rdd, child.schema)
    val aggExprs = zCols.flatMap {
      col =>
        Seq(col -> "min", col -> "max")
    }
    val minmaxRow: Array[Row] = df.agg(aggExprs.head, aggExprs.tail: _*).collect()
    // minmaxRow(0) => aggregation returns one row
    val minmax = minmaxRow(0)

    val zorderExprs = zCols.zipWithIndex.map {
      case (expr, index) =>
        Tuple3(expr, minmax.getAs[Number](index * 2),
          minmax.getAs[Number]((index * 2) + 1))
    }

    // TODO: Add a test case
    df.zorderBy(zorderExprs(0), zorderExprs(1), zorderExprs.drop(2): _*)
  }
}
