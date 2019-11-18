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

import org.apache.spark.sql.internal.SQLConf

private[spark] object SkewUtils {

  val SKEW: String = "skew"
  /**
    * Enable skew join only when spark.sql.execution.sortBeforeRepartition
    * otherwise shuffle write will become expensive due to skew value computation.
    * why? If data is not sorted we need to hold the information
    * of all the records in memory  along with their frequency
    * and then retrieve the skew values at the end of Shuffle write.
    * This operation is expensive mainly in terms of memory
    * and will cause ShuffleMapTask slowdown, OOM etc
    * @return true, if skew can be handled
    */
  def canHandleSkew(): Boolean = {
    val conf = SQLConf.get
    conf.sortBeforeRepartition &&
      conf.adaptiveExecutionEnabled
  }
}
