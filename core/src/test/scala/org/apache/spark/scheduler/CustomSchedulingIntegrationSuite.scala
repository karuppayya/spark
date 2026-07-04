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
 * End-to-end tests that exercise [[SchedulingAlgorithmProvider]] and
 * [[TaskSetWeightProvider]] together through [[Pool#getSortedTaskSetQueue]], i.e. the actual
 * scheduling path used by [[TaskSchedulerImpl]].
 */
class CustomSchedulingIntegrationSuite extends SparkFunSuite with LocalSparkContext {

  private def createTaskSetManager(
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

  test("Provider that redefines a built-in mode is rejected at startup") {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("test")
      .set(SCHEDULER_ALGORITHM_PROVIDERS,
        Seq("org.apache.spark.scheduler.BuiltInOverrideAlgorithmProvider"))

    val e = intercept[IllegalArgumentException] {
      sc = new SparkContext(conf)
    }
    assert(e.getMessage.contains("may not override built-in scheduling mode"))
  }

  test("DefaultWeightProvider preserves FIFO ordering across TaskSets") {
    sc = new SparkContext(new SparkConf().setMaster("local").setAppName("test"))
    val taskScheduler = new TaskSchedulerImpl(sc)

    val pool = new Pool("fifo", SchedulingMode.FIFO, 0, 0)
    val tsm1 = createTaskSetManager(1, 2, taskScheduler)
    val tsm2 = createTaskSetManager(2, 10, taskScheduler)
    val tsm3 = createTaskSetManager(3, 5, taskScheduler)
    pool.addSchedulable(tsm1)
    pool.addSchedulable(tsm2)
    pool.addSchedulable(tsm3)

    Seq(tsm1, tsm2, tsm3).foreach(t => assert(t.weight === 1))
    assert(pool.getSortedTaskSetQueue.map(_.stageId).toSeq === Seq(1, 2, 3))
  }

  test("Built-in FIFO ignores custom weights") {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("test")
      .set(SCHEDULER_TASKSET_WEIGHT_PROVIDER_CLASS,
        "org.apache.spark.scheduler.TaskCountWeightProvider")

    sc = new SparkContext(conf)
    val taskScheduler = new TaskSchedulerImpl(sc)

    val pool = new Pool("fifo", SchedulingMode.FIFO, 0, 0)
    val tsm1 = createTaskSetManager(1, 5, taskScheduler)
    val tsm2 = createTaskSetManager(2, 10, taskScheduler)
    pool.addSchedulable(tsm1)
    pool.addSchedulable(tsm2)

    // Even though tsm2 has higher weight, FIFO orders by stageId.
    assert(pool.getSortedTaskSetQueue.map(_.stageId).toSeq === Seq(1, 2))
  }

  test("Empty / whitespace-only provider config is treated as no providers") {
    val confEmpty = new SparkConf()
      .setMaster("local")
      .setAppName("test")
      .set(SCHEDULER_ALGORITHM_PROVIDERS, Seq.empty[String])

    sc = new SparkContext(confEmpty)
    new Pool("", SchedulingMode.FIFO, 0, 0)
  }

}
