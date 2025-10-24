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

import org.apache.spark.sql.catalyst.plans.physical.PassThroughPartitioning
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.adaptive.ShuffleQueryStageExec
import org.apache.spark.sql.execution.exchange.{SHUFFLE_CONSOLIDATION, ShuffleExchangeExec}
import org.apache.spark.sql.internal.SQLConf

object AddConsolidationShuffle extends Rule[SparkPlan] {

  def apply(plan: SparkPlan): SparkPlan = {
    if (!SQLConf.get.shuffleConsolidationEnabled) {
      return plan
    }
    plan transformUp {
      case plan @ ShuffleExchangeExec(part, _, origin, _) =>
        // Non-adaptive: always add consolidation exchange for shuffle exchanges
        ShuffleExchangeExec(PassThroughPartitioning(part), plan, origin)
      case p: ShuffleQueryStageExec
        if !p.shuffle.outputPartitioning.isInstanceOf[PassThroughPartitioning] =>
        // Adaptive: only add consolidation exchange if the stage is both:
        // 1. Large enough to benefit from consolidation (exceeds consolidation threshold)
        // 2. Too large to be broadcast (exceeds broadcast threshold)
        // This ensures we don't consolidate stages that might be converted to broadcast joins,
        // which would prevent the SortMergeJoin -> BroadcastHashJoin conversion from happening.
        val size = p.getRuntimeStatistics.sizeInBytes
        val broadcastThreshold = SQLConf.get.getConf(SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD)
          .getOrElse(SQLConf.get.autoBroadcastJoinThreshold)
        val consolidationThreshold = SQLConf.get.shuffleConsolidationSizeThreshold
        if (size > consolidationThreshold && size > broadcastThreshold) {
          ShuffleExchangeExec(PassThroughPartitioning(p.outputPartitioning), p,
            SHUFFLE_CONSOLIDATION)
        } else {
          // Don't consolidate - stage might be small enough to be broadcast,
          // and consolidation would interfere with broadcast join conversion
          p
        }
    }
  }
}
