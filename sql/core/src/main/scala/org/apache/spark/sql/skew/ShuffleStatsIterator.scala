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

import scala.reflect.ClassTag

import org.apache.spark.skew.{ShufflePartitionInfo, StatsGetter}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.internal.SQLConf

/*
 * Iterator that wraps the shuffle records iterator to compute stats
 * The iterator implements org.apache.spark.skew.StatsGetter
 * org.apache.spark.skew.StatsGetter.getStats needs to return the statistics from
 * the records the iterated rows
 *
 * @param iter the iterator to be wrapped
 * @param numPartitions number of shuffle partitions
 * @param unsafeValueExtractor extracts the value of partition keys for a record
 */
private[spark] class ShuffleStatsIterator[T <: Product2[Int, InternalRow], V <: InternalRow](
   iter: Iterator[T],
   numPartitions: Int,
   unsafeValueExtractor: T => V,
   safeValueExtractor: V => V)(implicit tag: ClassTag[V]) extends Iterator[T] with StatsGetter {

  private val statsHolder: Seq[StatsHolderInterface[V]] = {
    val queueSize: Int = SQLConf.get.skewQueueSize
    List.fill(numPartitions) {
      if (queueSize == 1) {
        new SingleSkewValueHolder[V]()
      } else {
        new MultiSkewValueHolder[V](queueSize)
      }
    }
  }

  private var stats: Option[Seq[ShufflePartitionInfo]] = None

  override def hasNext: Boolean = iter.hasNext

  override def next(): T = {
    val value = iter.next
    statsHolder(value._1).update(unsafeValueExtractor(value))
    value
  }

  override def getStats: Option[Seq[ShufflePartitionInfo]] = {
    if (stats.isEmpty) {
      stats = Option(statsHolder.map {
        statsHolder =>
          statsHolder.close()
          val stats = statsHolder.getStats.map {
            statsInfo =>
              statsInfo.copy(safeValueExtractor(statsInfo.obj.asInstanceOf[V]))
          }
          ShufflePartitionInfo(stats, statsHolder.getRecordsCount)
      })
    }
    stats
  }

  // required for unit test
  private[spark] def getStatsHolder = statsHolder
}
