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

package org.apache.spark.sql.execution.exchange

import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{AddConsolidationShuffle, ColumnarRule, SparkPlan}
import org.apache.spark.sql.internal.SQLConf

/**
 * SparkSessionExtensions that injects the AddConsolidationShuffle rule
 * into both AQE and non-AQE execution paths.
 *
 * - Non-AQE: injected via injectColumnar (preColumnarTransitions), which runs inside
 *   ApplyColumnarRulesAndInsertTransitions in QueryExecution.preparations.
 * - AQE: injected via injectQueryStagePrepRule, which appends to
 *   AdaptiveSparkPlanExec.queryStagePreparationRules.
 *
 * The columnar path is guarded to only apply when AQE is disabled, since
 * ApplyColumnarRulesAndInsertTransitions also runs in AQE's postStageCreationRules
 * where the plan context differs from the preparation phase.
 *
 * Configure via:
 * {{{
 *   spark.sql.extensions=org.apache.spark.sql.execution.exchange.ShuffleVaultExtensions
 * }}}
 */
class ShuffleVaultExtensions extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    // Non-AQE path: runs via ApplyColumnarRulesAndInsertTransitions in preparations.
    // Guarded to skip when AQE is enabled since that path is handled separately.
    extensions.injectColumnar(_ => new ColumnarRule {
      override def preColumnarTransitions: Rule[SparkPlan] = (plan: SparkPlan) => {
        if (SQLConf.get.adaptiveExecutionEnabled) {
          plan
        } else {
          AddConsolidationShuffle.apply(plan)
        }
      }
    })
    // AQE path: runs via queryStagePreparationRules in AdaptiveSparkPlanExec
    extensions.injectQueryStagePrepRule(_ => AddConsolidationShuffle)
  }
}
