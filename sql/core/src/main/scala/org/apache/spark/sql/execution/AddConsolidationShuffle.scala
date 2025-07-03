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
import org.apache.spark.sql.execution.exchange.{ENSURE_REQUIREMENTS, ShuffleExchangeExec}
import org.apache.spark.sql.internal.SQLConf

object AddConsolidationShuffle extends Rule[SparkPlan] {

  def apply(plan: SparkPlan): SparkPlan = {
    if (!SQLConf.get.additionalShuffleStage) {
      return plan
    }
    plan transformUp {
      case plan @ ShuffleExchangeExec(part, _, origin, _) if SQLConf.get.additionalShuffleStage =>
        // Non adaptive
        ShuffleExchangeExec(PassThroughPartitioning(part), plan,
          origin)
      case p: ShuffleQueryStageExec
        if !p.shuffle.outputPartitioning.isInstanceOf[PassThroughPartitioning] =>
        // Adaptive
        ShuffleExchangeExec(PassThroughPartitioning(p.outputPartitioning), p,
          ENSURE_REQUIREMENTS)
    }
  }
}
