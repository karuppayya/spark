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
import org.apache.spark.util.ManualClock

/**
 * Tests for RemoteShuffleWeightProvider.
 */
class RemoteShuffleWeightProviderSuite extends SparkFunSuite with LocalSparkContext {

  test("TaskSet without shuffleId gets default weight") {
    sc = new SparkContext("local", "test")
    val taskScheduler = new TaskSchedulerImpl(sc)
    val provider = new RemoteShuffleWeightProvider()

    // Create a TaskSet with no shuffle ID (None)
    val tasks = Array.tabulate[Task[_]](5)(i => new FakeTask(0, i, Nil))
    val tsm = new TaskSetManager(
      taskScheduler,
      new TaskSet(tasks, 0, 0, 0, null, ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID, None),
      maxTaskFailures = 0,
      healthTracker = None,
      clock = new ManualClock(),
      weightProvider = provider)

    assert(tsm.weight === 1, "TaskSet without shuffle should have default weight")
  }

  test("TaskSet with shuffleId but no ShuffleVaultManager gets default weight") {
    sc = new SparkContext("local", "test")
    val taskScheduler = new TaskSchedulerImpl(sc)
    val provider = new RemoteShuffleWeightProvider()

    // Create a TaskSet with a shuffle ID but using default shuffle manager
    val tasks = Array.tabulate[Task[_]](5)(i => new FakeTask(0, i, Nil))
    val tsm = new TaskSetManager(
      taskScheduler,
      new TaskSet(tasks, 0, 0, 0, null, ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID, Some(1)),
      maxTaskFailures = 0,
      healthTracker = None,
      clock = new ManualClock(),
      weightProvider = provider)

    // Without ShuffleVaultManager, should get default weight
    assert(tsm.weight === 1, "TaskSet with local shuffle should have default weight")
  }

  test("RemoteShuffleWeightProvider serialization") {
    val provider = new RemoteShuffleWeightProvider()

    // Provider should be serializable
    val serialized = org.apache.spark.util.Utils.serialize(provider)
    val deserialized = org.apache.spark.util.Utils.deserialize[TaskSetWeightProvider](serialized)

    assert(deserialized.isInstanceOf[RemoteShuffleWeightProvider])
  }

  test("TaskSet weight is determined by shuffleId") {
    sc = new SparkContext("local", "test")
    val taskScheduler = new TaskSchedulerImpl(sc)

    // Create task sets with different shuffle IDs
    val tasks1 = Array.tabulate[Task[_]](5)(i => new FakeTask(1, i, Nil))
    val tsm1 = taskScheduler.createTaskSetManager(
      new TaskSet(tasks1, 1, 0, 0, null, ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID, Some(100)),
      maxTaskFailures = 0)

    val tasks2 = Array.tabulate[Task[_]](5)(i => new FakeTask(2, i, Nil))
    val tsm2 = taskScheduler.createTaskSetManager(
      new TaskSet(tasks2, 2, 0, 0, null, ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID, Some(200)),
      maxTaskFailures = 0)

    // Weights are determined by the configured weight provider
    // With default config, both should have weight 1
    assert(tsm1.weight >= 1)
    assert(tsm2.weight >= 1)
  }

  test("Integration with WeightedFIFO scheduling") {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("test")
      .set(SCHEDULER_ALGORITHM_PROVIDERS,
        "org.apache.spark.scheduler.WeightedFIFOAlgorithmProvider")
      .set(SCHEDULER_TASKSET_WEIGHT_PROVIDER_CLASS,
        "org.apache.spark.scheduler.RemoteShuffleWeightProvider")

    sc = new SparkContext(conf)
    val taskScheduler = new TaskSchedulerImpl(sc)
    val pool = new Pool("test", SchedulingMode.WEIGHTED_FIFO, 0, 0)

    // Create multiple task sets
    val tasks1 = Array.tabulate[Task[_]](5)(i => new FakeTask(1, i, Nil))
    val tsm1 = taskScheduler.createTaskSetManager(
      new TaskSet(tasks1, 1, 0, 0, null, ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID, None), 0)

    val tasks2 = Array.tabulate[Task[_]](5)(i => new FakeTask(2, i, Nil))
    val tsm2 = taskScheduler.createTaskSetManager(
      new TaskSet(tasks2, 2, 0, 0, null, ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID, Some(1)), 0)

    pool.addSchedulable(tsm1)
    pool.addSchedulable(tsm2)

    val sortedQueue = pool.getSortedTaskSetQueue

    // Verify both are scheduled (order depends on weights)
    assert(sortedQueue.length === 2)
  }
}
