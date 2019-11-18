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

import java.util.PriorityQueue

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import org.apache.spark.skew.{Metric, StatInfo}
import org.apache.spark.sql.catalyst.InternalRow

/**
* An Priority queue based implementation to capture skew
* information.
* It internally uses com.google.common.collect.MinMaxPriorityQueue
*
*/
private[spark] class MultiSkewValueHolder[V <: InternalRow](
    queueSize: Int)(implicit a: ClassTag[V]) extends StatsHolderInterface[V] {

  private var numRecords: Long = 0L
  private var currentCount: Long = 1
  private var currentValue: V = null.asInstanceOf[V]
  private var closed: Boolean = false

  private val queue: PriorityQueue[StatInfo] = {
    import java.util.Comparator
    val comparator = new Comparator[StatInfo]() {
      override def compare(o1: StatInfo, o2: StatInfo): Int =
        (o2.getMetric("count").asInstanceOf[Long] - o1.getMetric("count").asInstanceOf[Long]).toInt
    }
    new PriorityQueue[StatInfo](3, comparator) {
      override def add(e: StatInfo): Boolean = {
        if (size() == queueSize) {
          val minCount = peek().getMetric(StatInfo.COUNT_METRIC_NAME).asInstanceOf[Long]
          if (minCount > e.getMetric(StatInfo.COUNT_METRIC_NAME).asInstanceOf[Long]) {
            return false
          } else {
            poll()
          }
        }
        super.add(e)
      }
    }
  }

  override def update(value: V): Unit = {
    numRecords += 1
    if (currentValue == null) {
      currentValue = value.copy().asInstanceOf[V]
      currentCount = 0
    }
    if (currentValue != value) {
      val metrics = Seq(Metric("count", currentCount))
      queue.add(new StatInfo(currentValue, metrics))
      currentValue = value.copy().asInstanceOf[V]
      currentCount = 0
    }
    currentCount += 1
  }

  override def getStatsInternal: Seq[StatInfo] = {
    val queueIter = queue.iterator()
    val stats: ArrayBuffer[StatInfo] = ArrayBuffer.empty
    while (queueIter.hasNext) {
      stats += queueIter.next
    }
    stats
  }

  override def close(): Unit = {
    closed = true
    if (currentValue != null) {
      val metrics = Seq(Metric("count", currentCount))
      queue.add(new StatInfo(currentValue, metrics))
    }
  }

  override def reset(): Unit = {
    currentValue = null.asInstanceOf[V]
    currentCount = 1
    numRecords = 0
    closed = false
    queue.clear()
  }

  override def isClosed: Boolean = closed

  override def getRecordsCount: Long = numRecords
}
