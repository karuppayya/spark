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

import org.apache.spark.skew.StatInfo


/*
 * Interface used to collect stats
 *
 */
private[spark] trait StatsHolderInterface[V] {

  /*
   * Called for every record in the mapper partition
   */
  def update(value: V)

  /*
   * Gets stats for the mapper partition
   */
  def getStatsInternal: Seq[StatInfo]

  /*
   * Closes the holder
   */
  def close(): Unit

  def reset(): Unit

  /*
   * Checks whether holder is closed
   */
  def isClosed: Boolean


  def getStats: Seq[StatInfo] = {
    if (!isClosed) {
      throw new RuntimeException("Cannot get stats without closing the holder")
    }
    getStatsInternal
  }

  /*
   * Get number of records in this mapper partition
   */
  def getRecordsCount: Long
}
