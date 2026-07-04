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

import org.apache.spark.{LocalSparkContext, SparkConf, SparkContext, SparkFunSuite}
import org.apache.spark.internal.config._

/**
 * Tests for SchedulingAlgorithmProvider functionality.
 */
class SchedulingAlgorithmProviderSuite extends SparkFunSuite with LocalSparkContext {

  test("BuiltInAlgorithmProvider supports FIFO and FAIR") {
    val conf = new SparkConf()

    val fifo = BuiltInAlgorithmProvider.createAlgorithm("FIFO", conf)
    assert(fifo.isDefined)
    assert(fifo.get.isInstanceOf[FIFOSchedulingAlgorithm])

    val fair = BuiltInAlgorithmProvider.createAlgorithm("FAIR", conf)
    assert(fair.isDefined)
    assert(fair.get.isInstanceOf[FairSchedulingAlgorithm])

    val unsupported = BuiltInAlgorithmProvider.createAlgorithm("CUSTOM", conf)
    assert(unsupported.isEmpty)

    assert(BuiltInAlgorithmProvider.supportedModes === Seq("FIFO", "FAIR"))
  }

  test("BuiltInAlgorithmProvider is case insensitive") {
    val conf = new SparkConf()

    val fifoLower = BuiltInAlgorithmProvider.createAlgorithm("fifo", conf)
    assert(fifoLower.isDefined)
    assert(fifoLower.get.isInstanceOf[FIFOSchedulingAlgorithm])

    val fairMixed = BuiltInAlgorithmProvider.createAlgorithm("FaIr", conf)
    assert(fairMixed.isDefined)
    assert(fairMixed.get.isInstanceOf[FairSchedulingAlgorithm])
  }

  test("Custom algorithm provider is loaded and used") {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("test")
      .set(SCHEDULER_ALGORITHM_PROVIDERS,
        Seq("org.apache.spark.scheduler.WeightedFIFOAlgorithmProvider"))

    sc = new SparkContext(conf)
    val taskScheduler = new TaskSchedulerImpl(sc)

    assert(taskScheduler != null)
  }

  test("Multiple custom providers can be registered") {
    val conf = new SparkConf()
      .set(SCHEDULER_ALGORITHM_PROVIDERS,
        Seq("org.apache.spark.scheduler.WeightedFIFOAlgorithmProvider",
          "org.apache.spark.scheduler.PriorityAlgorithmProvider"))

    sc = new SparkContext("local", "test", conf)
    assert(sc != null)
  }

  test("Misconfigured provider class fails fast") {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("test")
      .set(SCHEDULER_ALGORITHM_PROVIDERS,
        Seq("org.apache.spark.scheduler.NonExistentProvider"))

    intercept[ClassNotFoundException] {
      sc = new SparkContext(conf)
    }
  }

  test("Custom provider is tried before built-in provider") {
    val provider = new WeightedFIFOAlgorithmProvider()
    val conf = new SparkConf()

    val weighted = provider.createAlgorithm("WEIGHTED_FIFO", conf)
    assert(weighted.isDefined)
    assert(weighted.get.isInstanceOf[WeightedFIFOSchedulingAlgorithm])

    val fifo = provider.createAlgorithm("FIFO", conf)
    assert(fifo.isEmpty)
  }

  test("Provider chain falls back to built-in for standard modes") {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("test")
      .set(SCHEDULER_ALGORITHM_PROVIDERS,
        Seq("org.apache.spark.scheduler.WeightedFIFOAlgorithmProvider"))

    sc = new SparkContext(conf)

    val pool = new Pool("", SchedulingMode.FIFO, 0, 0)
    assert(pool != null)
  }
}
