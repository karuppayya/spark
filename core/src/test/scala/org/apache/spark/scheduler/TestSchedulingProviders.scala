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
 * Shared test fixtures for [[SchedulingAlgorithmProvider]] and [[TaskSetWeightProvider]].
 *
 * These classes are reflectively loaded from configuration in multiple suites
 * (`SchedulingAlgorithmProviderSuite`, `TaskSetWeightProviderSuite`,
 * `CustomSchedulingIntegrationSuite`) and must be top-level public types reachable on the
 * test classpath, so they live here rather than inside any single suite file.
 */

/** Custom algorithm provider for testing that provides "WEIGHTED_FIFO" algorithm. */
class WeightedFIFOAlgorithmProvider extends SchedulingAlgorithmProvider {
  override def createAlgorithm(mode: String, conf: SparkConf): Option[SchedulingAlgorithm] = {
    if (mode.toUpperCase(Locale.ROOT) == "WEIGHTED_FIFO") {
      Some(new WeightedFIFOSchedulingAlgorithm())
    } else {
      None
    }
  }

  override def supportedModes: Seq[String] = Seq("WEIGHTED_FIFO")
}

/** Custom scheduling algorithm for testing that orders by weight then priority. */
class WeightedFIFOSchedulingAlgorithm extends SchedulingAlgorithm {
  override def comparator(s1: Schedulable, s2: Schedulable): Boolean = {
    if (s1.weight != s2.weight) {
      s1.weight > s2.weight
    } else if (s1.priority != s2.priority) {
      s1.priority < s2.priority
    } else {
      s1.stageId < s2.stageId
    }
  }
}

/** Another custom provider for testing multiple providers. */
class PriorityAlgorithmProvider extends SchedulingAlgorithmProvider {
  override def createAlgorithm(mode: String, conf: SparkConf): Option[SchedulingAlgorithm] = {
    if (mode.toUpperCase(Locale.ROOT) == "PRIORITY") {
      Some(new FIFOSchedulingAlgorithm())
    } else {
      None
    }
  }

  override def supportedModes: Seq[String] = Seq("PRIORITY")
}

/**
 * Illegal provider that attempts to redefine the built-in FAIR mode. Used to verify that loading
 * such a provider is rejected.
 */
class BuiltInOverrideAlgorithmProvider extends SchedulingAlgorithmProvider {
  override def createAlgorithm(mode: String, conf: SparkConf): Option[SchedulingAlgorithm] = {
    if (mode.toUpperCase(Locale.ROOT) == "FAIR") {
      Some(new WeightedFIFOSchedulingAlgorithm())
    } else {
      None
    }
  }

  override def supportedModes: Seq[String] = Seq("FAIR")
}

/**
 * Custom weight provider for testing that assigns weight based on number of tasks.
 * Weight = numTasks * 10
 */
class TaskCountWeightProvider extends TaskSetWeightProvider {
  override def getWeight(taskSet: TaskSetInfo): Int = taskSet.numTasks * 10
}

/** Custom weight provider that uses (stageId + 1) as weight, ensuring positivity. */
class StageIdBasedWeightProvider extends TaskSetWeightProvider {
  override def getWeight(taskSet: TaskSetInfo): Int = taskSet.stageId + 1
}
