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

package org.apache.spark.scheduler

/**
 * Trait for providing scheduling weights for TaskSets.
 *
 * Implementations can derive weights based on TaskSetManager context such as
 * task count, stage type, or other metadata.
 */
private[spark] trait TaskSetWeightProvider extends Serializable {
  /**
   * Calculate the scheduling weight for a TaskSetManager.
   * @return Weight as an integer (higher = higher priority)
   */
  def getWeight(taskSetManager: TaskSetManager): Int
}

/**
 * Default implementation that returns weight of 1 for all TaskSets.
 * This preserves current Spark behavior where all stages have equal weight.
 */
private[spark] class DefaultWeightProvider extends TaskSetWeightProvider {
  override def getWeight(taskSetManager: TaskSetManager): Int = 1
}
