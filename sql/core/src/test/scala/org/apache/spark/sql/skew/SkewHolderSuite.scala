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
import org.apache.spark.skew.{Metric, StatInfo}
import org.apache.spark.sql.catalyst.InternalRow

class SkewHolderSuite extends SparkFunSuite
  with BeforeAndAfterEach {

  val skewQueueSize: Int = 3

  val multiValueSkewHolder = new MultiSkewValueHolder[InternalRow](skewQueueSize)
  val singleValueSkewHolder = new SingleSkewValueHolder[InternalRow]()

  val keyHolders: Seq[StatsHolderInterface[InternalRow]] = Seq(
    new MultiSkewValueHolder[InternalRow](skewQueueSize),
    new SingleSkewValueHolder[InternalRow]()
  )

  test("test queue based skew holder for multiple keys") {
    val values = toInternalRows(Seq(0, 1, 1, 1))
    values.foreach(value => multiValueSkewHolder.update(value))
    multiValueSkewHolder.close()
    val skewedKeys = multiValueSkewHolder.getStats
    assert(skewedKeys.length == 2)

    val expectedInfo = Seq(StatInfo(InternalRow(1), Seq(Metric("count", 3))),
      StatInfo(InternalRow(0), Seq(Metric("count", 1))))
    assert(skewedKeys == expectedInfo)
  }

  test("test sort based skew holder for multiple keys") {
    val values = toInternalRows(Seq(0, 1, 1, 1))
    values.foreach(value => singleValueSkewHolder.update(value))
    singleValueSkewHolder.close()
    val skewedKeys = singleValueSkewHolder.getStats
    assert(skewedKeys.length == 1)

    val expectedInfo = Seq(StatInfo(InternalRow(1), Seq(Metric("count", 3))))
    assert(skewedKeys == expectedInfo)
  }

  test("test skew holders for single value") {
    keyHolders.foreach { holder =>
      val values = toInternalRows(Seq(1, 1, 1, 1))
      values.foreach(value => holder.update(value))
      holder.close()
      val skewedKeys = holder.getStats
      assert(skewedKeys.length == 1)
      val expectedInfo = Seq(StatInfo(InternalRow(1), Seq(Metric("count", 4))))
      assert(skewedKeys == expectedInfo)
    }
  }

  test("test skew holders for record count") {
    case class ValueHolder(values: Seq[InternalRow], count: Int)
    Seq(
      ValueHolder(toInternalRows(Seq(1, 1, 1, 1)), 4),
      ValueHolder(toInternalRows(Seq(1)), 1),
      ValueHolder(Seq(), 0)
    ).foreach {
      testCase =>
        keyHolders.foreach { holder =>
          holder.reset()
          val values = testCase.values
            values.foreach(value => holder.update(value))
          holder.close()
          assert(holder.getRecordsCount == testCase.count)
        }
    }

  }

  test("test skew holder for no values") {
    keyHolders.foreach { holder =>
        holder.close()
        val skewedKeys = holder.getStats
        assert(skewedKeys.isEmpty)
    }
  }

  test("should not be able to get skew values without closing holder") {
    assertThrows[RuntimeException] {
      val holder = new MultiSkewValueHolder[InternalRow](skewQueueSize)
      holder.getStats
    }
  }

  def toInternalRows(values: Seq[Int]): Seq[InternalRow] = {
    values.map {
      value =>
        InternalRow.apply(value)
    }
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    keyHolders.foreach(_.reset())
  }
}

