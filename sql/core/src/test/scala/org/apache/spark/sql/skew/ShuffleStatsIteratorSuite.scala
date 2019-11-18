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

package org.apache.spark.sql.skew

import org.scalatest.BeforeAndAfterEach

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.LazilyGeneratedOrdering
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.types.StructType

class ShuffleStatsIteratorSuite extends SparkFunSuite
  with SQLHelper with BeforeAndAfterEach {

  private def valueExtractor: Product2[Int, InternalRow] => InternalRow = {
    row =>
      InternalRow.fromSeq(Seq(row._2.getInt(0)))
  }

  private def safeValueExtractor: InternalRow => InternalRow = identity[InternalRow]

  test("Check the implementation of stat holder") {
    withSQLConf("spark.sql.qubole.skew.holder.queue.size" -> "1") {
      val iter = (1 to 10).map {
        index =>
          index -> InternalRow.fromSeq(Seq(index))
      }.iterator

      val newIter = new ShuffleStatsIterator(iter, 100, valueExtractor, safeValueExtractor)
      assert(newIter.getStatsHolder.isInstanceOf[Seq[SingleSkewValueHolder[_]]],
        "Wrong holder created")

    }
    withSQLConf("spark.sql.qubole.skew.holder.queue.size" -> "2") {
      val iter = (1 to 10).map {
        index =>
          index -> InternalRow.fromSeq(Seq(index))
      }.iterator

      val newIter = new ShuffleStatsIterator(iter, 100, valueExtractor, safeValueExtractor)
      assert(newIter.getStatsHolder.isInstanceOf[Seq[MultiSkewValueHolder[_]]],
        "Wrong holder created")
    }
  }

  test("Custom iterator should return same values as the iterator that it is wrapping") {
    val intSeq = 1 to 10
    val iter = intSeq.map {
      index =>
        index -> InternalRow.fromSeq(Seq(index))
    }.iterator

    val customIter = new ShuffleStatsIterator(iter, 100, valueExtractor, safeValueExtractor)
    assert(customIter.map(_._2.getInt(0)).toSeq == intSeq)
  }
}
