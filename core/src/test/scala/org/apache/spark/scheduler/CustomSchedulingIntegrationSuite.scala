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
import org.apache.spark.resource.ResourceProfile

/**
 * Integration tests for custom scheduling algorithm and weight providers.
 */
class CustomSchedulingIntegrationSuite extends SparkFunSuite with LocalSparkContext {

  def createTaskSetManager(
      stageId: Int,
      numTasks: Int,
      taskScheduler: TaskSchedulerImpl): TaskSetManager = {
    val tasks = Array.tabulate[Task[_]](numTasks) { i =>
      new FakeTask(stageId, i, Nil)
    }
    taskScheduler.createTaskSetManager(
      new TaskSet(tasks, stageId, 0, 0, null,
        ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID, None), 0)
  }

  test("Custom weight provider sets correct weights") {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("test")
      .set(SCHEDULER_TASKSET_WEIGHT_PROVIDER_CLASS,
        "org.apache.spark.scheduler.TaskCountWeightProvider")

    sc = new SparkContext(conf)
    val taskScheduler = new TaskSchedulerImpl(sc)

    // Create task sets with different sizes
    // TaskCountWeightProvider will assign weight = numTasks * 10
    val tsm1 = createTaskSetManager(1, 2, taskScheduler)  // weight = 20
    val tsm2 = createTaskSetManager(2, 10, taskScheduler) // weight = 100
    val tsm3 = createTaskSetManager(3, 5, taskScheduler)  // weight = 50

    // Verify weights are set correctly by the custom provider
    assert(tsm1.weight === 20)
    assert(tsm2.weight === 100)
    assert(tsm3.weight === 50)
  }

  test("Default weight provider maintains equal priority for all stages") {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("test")
    // No custom weight provider configured

    sc = new SparkContext(conf)
    val taskScheduler = new TaskSchedulerImpl(sc)

    val rootPool = new Pool("", SchedulingMode.FAIR, 0, 0)

    val tsm1 = createTaskSetManager(1, 2, taskScheduler)
    val tsm2 = createTaskSetManager(2, 10, taskScheduler)
    val tsm3 = createTaskSetManager(3, 5, taskScheduler)

    rootPool.addSchedulable(tsm1)
    rootPool.addSchedulable(tsm2)
    rootPool.addSchedulable(tsm3)

    // All should have weight = 1 (default)
    assert(tsm1.weight === 1)
    assert(tsm2.weight === 1)
    assert(tsm3.weight === 1)
  }

  test("Built-in algorithms still work with custom weight provider") {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("test")
      .set(SCHEDULER_TASKSET_WEIGHT_PROVIDER_CLASS,
        "org.apache.spark.scheduler.TaskCountWeightProvider")

    sc = new SparkContext(conf)
    val taskScheduler = new TaskSchedulerImpl(sc)

    // Test FIFO pool
    val fifoPool = new Pool("fifo", SchedulingMode.FIFO, 0, 0)
    val tsm1 = createTaskSetManager(1, 5, taskScheduler)
    val tsm2 = createTaskSetManager(2, 10, taskScheduler)

    fifoPool.addSchedulable(tsm1)
    fifoPool.addSchedulable(tsm2)

    val sortedQueue = fifoPool.getSortedTaskSetQueue
    // FIFO should maintain submission order regardless of weight
    assert(sortedQueue(0).stageId === 1)
    assert(sortedQueue(1).stageId === 2)
  }

  test("Empty provider configuration string is handled correctly") {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("test")
      .set(SCHEDULER_ALGORITHM_PROVIDERS, "")

    sc = new SparkContext(conf)
    val taskScheduler = new TaskSchedulerImpl(sc)

    // Should work with built-in algorithms
    val pool = new Pool("", SchedulingMode.FIFO, 0, 0)
    assert(pool != null)
  }

  test("Provider configuration with whitespace is handled correctly") {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("test")
      .set(SCHEDULER_ALGORITHM_PROVIDERS, " , , ")

    sc = new SparkContext(conf)
    val taskScheduler = new TaskSchedulerImpl(sc)

    // Should work with built-in algorithms
    val pool = new Pool("", SchedulingMode.FIFO, 0, 0)
    assert(pool != null)
  }

  test("Mixed valid and invalid providers") {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("test")
      .set(SCHEDULER_ALGORITHM_PROVIDERS,
        "org.apache.spark.scheduler.NonExistent," +
        "org.apache.spark.scheduler.WeightedFIFOAlgorithmProvider")

    sc = new SparkContext(conf)
    // Should successfully load with valid provider
    assert(sc != null)
  }

  test("Weight provider serialization") {
    val provider = new TaskCountWeightProvider()

    // Provider should be serializable
    val serialized = org.apache.spark.util.Utils.serialize(provider)
    val deserialized = org.apache.spark.util.Utils.deserialize[TaskSetWeightProvider](serialized)

    assert(deserialized.isInstanceOf[TaskCountWeightProvider])
  }

  test("Default weight provider serialization") {
    val provider = new DefaultWeightProvider()

    // Provider should be serializable
    val serialized = org.apache.spark.util.Utils.serialize(provider)
    val deserialized = org.apache.spark.util.Utils.deserialize[TaskSetWeightProvider](serialized)

    assert(deserialized.isInstanceOf[DefaultWeightProvider])
  }

  test("Custom algorithm with custom weight provider") {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("test")
      .set(SCHEDULER_ALGORITHM_PROVIDERS,
        "org.apache.spark.scheduler.WeightedFIFOAlgorithmProvider")
      .set(SCHEDULER_TASKSET_WEIGHT_PROVIDER_CLASS,
        "org.apache.spark.scheduler.TaskCountWeightProvider")

    sc = new SparkContext(conf)
    val taskScheduler = new TaskSchedulerImpl(sc)

    // Both custom providers should work together
    val tsm1 = createTaskSetManager(1, 5, taskScheduler)
    val tsm2 = createTaskSetManager(2, 5, taskScheduler)

    // Weight should come from TaskCountWeightProvider (numTasks * 10)
    assert(tsm1.weight === 50)
    assert(tsm2.weight === 50)
  }
}
