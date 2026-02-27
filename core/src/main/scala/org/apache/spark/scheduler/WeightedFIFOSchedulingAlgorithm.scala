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

import java.util.Locale

import org.apache.spark.SparkConf

/**
 * WeightedFIFO scheduling algorithm that orders TaskSets by:
 * 1. Priority (lower value = higher priority)
 * 2. Weight (higher value = higher priority)
 * 3. Stage ID (lower value = higher priority)
 *
 * This algorithm extends FIFO scheduling by considering the weight of each TaskSet,
 * allowing certain stages to be prioritized over others.
 */
private[spark] class WeightedFIFOSchedulingAlgorithm extends SchedulingAlgorithm {
  override def comparator(s1: Schedulable, s2: Schedulable): Boolean = {
    val priority1 = s1.priority
    val priority2 = s2.priority
    var res = math.signum(priority1 - priority2)
    if (res == 0) {
      if (s1.weight == s2.weight) {
        val stageId1 = s1.stageId
        val stageId2 = s2.stageId
        res = math.signum(stageId1 - stageId2)
      } else {
        // Higher the weight, earlier should it run (unlike priority)
        res = math.signum(s2.weight - s1.weight)
      }
    }
    res < 0
  }
}

/**
 * Provider for WeightedFIFO scheduling algorithm.
 * This provider can be registered via spark.scheduler.algorithm.providers configuration.
 */
private[spark] class WeightedFIFOAlgorithmProvider extends SchedulingAlgorithmProvider {
  override def createAlgorithm(mode: String, conf: SparkConf): Option[SchedulingAlgorithm] = {
    if (mode.toUpperCase(Locale.ROOT) == "WEIGHTED_FIFO") {
      Some(new WeightedFIFOSchedulingAlgorithm())
    } else {
      None
    }
  }

  override def supportedModes(): Seq[String] = Seq("WEIGHTED_FIFO")
}
