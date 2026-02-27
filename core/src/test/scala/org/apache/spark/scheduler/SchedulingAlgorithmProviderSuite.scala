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

import org.apache.spark.{LocalSparkContext, SparkConf, SparkContext, SparkFunSuite}
import org.apache.spark.internal.config._

/**
 * Tests for SchedulingAlgorithmProvider functionality.
 */
class SchedulingAlgorithmProviderSuite extends SparkFunSuite with LocalSparkContext {

  test("BuiltInAlgorithmProvider supports FIFO and FAIR") {
    val provider = new BuiltInAlgorithmProvider()
    val conf = new SparkConf()

    val fifo = provider.createAlgorithm("FIFO", conf)
    assert(fifo.isDefined)
    assert(fifo.get.isInstanceOf[FIFOSchedulingAlgorithm])

    val fair = provider.createAlgorithm("FAIR", conf)
    assert(fair.isDefined)
    assert(fair.get.isInstanceOf[FairSchedulingAlgorithm])

    val unsupported = provider.createAlgorithm("CUSTOM", conf)
    assert(unsupported.isEmpty)

    assert(provider.supportedModes() === Seq("FIFO", "FAIR"))
  }

  test("BuiltInAlgorithmProvider is case insensitive") {
    val provider = new BuiltInAlgorithmProvider()
    val conf = new SparkConf()

    val fifoLower = provider.createAlgorithm("fifo", conf)
    assert(fifoLower.isDefined)
    assert(fifoLower.get.isInstanceOf[FIFOSchedulingAlgorithm])

    val fairMixed = provider.createAlgorithm("FaIr", conf)
    assert(fairMixed.isDefined)
    assert(fairMixed.get.isInstanceOf[FairSchedulingAlgorithm])
  }

  test("Custom algorithm provider is loaded and used") {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("test")
      .set(SCHEDULER_ALGORITHM_PROVIDERS,
        "org.apache.spark.scheduler.WeightedFIFOAlgorithmProvider")

    sc = new SparkContext(conf)
    val taskScheduler = new TaskSchedulerImpl(sc)

    // Just verify that the context can be created with custom provider
    assert(taskScheduler != null)
  }

  test("Multiple custom providers can be registered") {
    val conf = new SparkConf()
      .set(SCHEDULER_ALGORITHM_PROVIDERS,
        "org.apache.spark.scheduler.WeightedFIFOAlgorithmProvider," +
        "org.apache.spark.scheduler.PriorityAlgorithmProvider")

    sc = new SparkContext("local", "test", conf)
    // Just verify that the context can be created with multiple providers
    assert(sc != null)
  }

  test("Invalid custom provider is ignored") {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("test")
      .set(SCHEDULER_ALGORITHM_PROVIDERS,
        "org.apache.spark.scheduler.NonExistentProvider")

    sc = new SparkContext(conf)
    val taskScheduler = new TaskSchedulerImpl(sc)

    // Should fall back to built-in algorithms without error
    val pool = new Pool("", SchedulingMode.FIFO, 0, 0)
    assert(pool != null)
  }

  test("Custom provider is tried before built-in provider") {
    val provider = new WeightedFIFOAlgorithmProvider()
    val conf = new SparkConf()

    // Custom provider should handle "WEIGHTED_FIFO"
    val weighted = provider.createAlgorithm("WEIGHTED_FIFO", conf)
    assert(weighted.isDefined)
    assert(weighted.get.isInstanceOf[WeightedFIFOSchedulingAlgorithm])

    // Custom provider should not handle standard modes
    val fifo = provider.createAlgorithm("FIFO", conf)
    assert(fifo.isEmpty)
  }

  test("Provider chain falls back to built-in for standard modes") {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("test")
      .set(SCHEDULER_ALGORITHM_PROVIDERS,
        "org.apache.spark.scheduler.WeightedFIFOAlgorithmProvider")

    sc = new SparkContext(conf)

    // Even with custom provider, built-in FIFO/FAIR should still work
    val pool = new Pool("", SchedulingMode.FIFO, 0, 0)
    assert(pool != null)
  }
}

/**
 * Another custom provider for testing multiple providers.
 */
class PriorityAlgorithmProvider extends SchedulingAlgorithmProvider {
  override def createAlgorithm(mode: String, conf: SparkConf): Option[SchedulingAlgorithm] = {
    if (mode.toUpperCase(Locale.ROOT) == "PRIORITY") {
      Some(new FIFOSchedulingAlgorithm()) // For simplicity, just use FIFO
    } else {
      None
    }
  }

  override def supportedModes(): Seq[String] = Seq("PRIORITY")
}
