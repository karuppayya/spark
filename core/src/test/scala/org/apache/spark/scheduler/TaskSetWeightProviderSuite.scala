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
 * Tests for TaskSetWeightProvider functionality.
 */
class TaskSetWeightProviderSuite extends SparkFunSuite with LocalSparkContext {

  private def createTaskSetManager(
      stageId: Int,
      numTasks: Int,
      taskScheduler: TaskSchedulerImpl,
      weightProvider: TaskSetWeightProvider = DefaultWeightProvider): TaskSetManager = {
    val tasks = Array.tabulate[Task[_]](numTasks) { i =>
      new FakeTask(stageId, i, Nil)
    }
    new TaskSetManager(
      taskScheduler,
      new TaskSet(tasks, stageId, 0, 0, null, ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID, None),
      maxTaskFailures = 0,
      healthTracker = None,
      clock = new ManualClock(),
      weightProvider = weightProvider)
  }

  test("DefaultWeightProvider returns weight of 1") {
    sc = new SparkContext("local", "test")
    val taskScheduler = new TaskSchedulerImpl(sc)

    val tsm = createTaskSetManager(0, 5, taskScheduler, DefaultWeightProvider)
    assert(tsm.weight === 1)
  }

  test("DefaultWeightProvider is used when no configuration is set") {
    val conf = new SparkConf().setMaster("local").setAppName("test")
    sc = new SparkContext(conf)
    val taskScheduler = new TaskSchedulerImpl(sc)

    val tsm = taskScheduler.createTaskSetManager(
      new TaskSet(
        Array.tabulate[Task[_]](5)(i => new FakeTask(0, i, Nil)),
        0, 0, 0, null, ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID, None),
      maxTaskFailures = 0)

    assert(tsm.weight === 1)
  }

  test("Custom weight provider is loaded from configuration") {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("test")
      .set(SCHEDULER_TASKSET_WEIGHT_PROVIDER_CLASS,
        "org.apache.spark.scheduler.TaskCountWeightProvider")

    sc = new SparkContext(conf)
    val taskScheduler = new TaskSchedulerImpl(sc)

    val tsm = taskScheduler.createTaskSetManager(
      new TaskSet(
        Array.tabulate[Task[_]](10)(i => new FakeTask(0, i, Nil)),
        0, 0, 0, null, ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID, None),
      maxTaskFailures = 0)

    // TaskCountWeightProvider returns number of tasks * 10
    assert(tsm.weight === 100)
  }

  test("Misconfigured weight provider class fails fast") {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("test")
      .set(SCHEDULER_TASKSET_WEIGHT_PROVIDER_CLASS,
        "org.apache.spark.scheduler.NonExistentWeightProvider")

    intercept[ClassNotFoundException] {
      sc = new SparkContext(conf)
      new TaskSchedulerImpl(sc)
    }
  }

  test("Non-positive weights are rejected") {
    sc = new SparkContext("local", "test")
    val taskScheduler = new TaskSchedulerImpl(sc)
    val zeroProvider = new TaskSetWeightProvider {
      override def getWeight(taskSet: TaskSetInfo): Int = 0
    }
    val tsm = createTaskSetManager(0, 1, taskScheduler, zeroProvider)
    intercept[IllegalArgumentException] {
      tsm.weight
    }
  }

  test("Custom weight provider sets correct weights") {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("test")
      .set(SCHEDULER_TASKSET_WEIGHT_PROVIDER_CLASS,
        "org.apache.spark.scheduler.TaskCountWeightProvider")

    sc = new SparkContext(conf)
    val taskScheduler = new TaskSchedulerImpl(sc)

    val tsm1 = taskScheduler.createTaskSetManager(
      new TaskSet(Array.tabulate[Task[_]](1)(i => new FakeTask(1, i, Nil)),
        1, 0, 0, null, ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID, None), 0)
    val tsm2 = taskScheduler.createTaskSetManager(
      new TaskSet(Array.tabulate[Task[_]](5)(i => new FakeTask(2, i, Nil)),
        2, 0, 0, null, ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID, None), 0)
    val tsm3 = taskScheduler.createTaskSetManager(
      new TaskSet(Array.tabulate[Task[_]](10)(i => new FakeTask(3, i, Nil)),
        3, 0, 0, null, ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID, None), 0)

    assert(tsm1.weight === 10)
    assert(tsm2.weight === 50)
    assert(tsm3.weight === 100)
  }

  test("Weight provider with different weights for different stages") {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("test")
      .set(SCHEDULER_TASKSET_WEIGHT_PROVIDER_CLASS,
        "org.apache.spark.scheduler.StageIdBasedWeightProvider")

    sc = new SparkContext(conf)
    val taskScheduler = new TaskSchedulerImpl(sc)

    val tsm1 = taskScheduler.createTaskSetManager(
      new TaskSet(Array.tabulate[Task[_]](5)(i => new FakeTask(1, i, Nil)),
        1, 0, 0, null, ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID, None), 0)
    val tsm2 = taskScheduler.createTaskSetManager(
      new TaskSet(Array.tabulate[Task[_]](5)(i => new FakeTask(5, i, Nil)),
        5, 0, 0, null, ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID, None), 0)
    val tsm3 = taskScheduler.createTaskSetManager(
      new TaskSet(Array.tabulate[Task[_]](5)(i => new FakeTask(10, i, Nil)),
        10, 0, 0, null, ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID, None), 0)

    // StageIdBasedWeightProvider returns stageId + 1 (must be > 0)
    assert(tsm1.weight === 2)
    assert(tsm2.weight === 6)
    assert(tsm3.weight === 11)
  }
}
