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

import org.apache.spark.skew.{Metric, StatInfo}

/*
 *
 * An implementation to capture a single skewed key
 * per reducer partition
 * The implementation takes advantage of the sortedness of the data to
 * find the skew values.
 *
 */
private[spark] class SingleSkewValueHolder[V](implicit a: ClassTag[V])
  extends StatsHolderInterface[V] {

    private var numRecords: Long = 0L
    private var maxValue: V = null.asInstanceOf[V]
    private var maxFreq: Long = -1

    private var currentValue: V = null.asInstanceOf[V]
    private var curFreq = 0

    private var closed = false

    override def update(value: V): Unit = {
      if (currentValue == null) {
        currentValue = value
        curFreq = 0
      }

      if (currentValue.hashCode() != value.hashCode()) {
        if (curFreq > maxFreq) {
          maxValue = currentValue
          maxFreq = curFreq
        }
        currentValue = value
        curFreq = 0
      }
      curFreq += 1
      numRecords += 1
    }


    override def getStatsInternal: Seq[StatInfo] = if (this.maxValue != null) {
      val metric = Metric("count", this.maxFreq)
      Seq(StatInfo(this.maxValue, Seq(metric)))
    } else Seq.empty

    override def close(): Unit = {
      closed = true
      if (curFreq > maxFreq) {
        maxValue = currentValue
        maxFreq = curFreq
      }

    }

    override def reset(): Unit = {
      this.maxValue = null.asInstanceOf[V]
      this.currentValue = null.asInstanceOf[V]
      this.maxFreq = -1
      closed = false
      numRecords = 0

    }

    override def isClosed: Boolean = closed

    override def getRecordsCount: Long = numRecords
}