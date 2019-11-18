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

package org.apache.spark.skew

private[spark] case class Metric(name: String, value: Any)

/*
 * Container for stats for a particular record
 *
 * @param obj the object for which stats are collected
 * @param count frequency of obj
 */
private[spark] case class StatInfo(obj: Any, metrics: Seq[Metric]) {

  def getMetric(name: String): Any = {
    metrics.find( _.name == name).map(_.value).getOrElse(0L)
  }
}

/*
 * Created per partition
 *
 */
private[spark] case class ShufflePartitionInfo(infos: Seq[StatInfo], var recordCount: Long = 0)

private[spark] object StatInfo {
  val COUNT_METRIC_NAME = "count"
}
