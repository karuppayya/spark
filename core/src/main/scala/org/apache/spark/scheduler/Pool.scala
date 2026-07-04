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

import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.util.Utils

private[spark] object Pool {
  /**
   * Resolve a [[SchedulingAlgorithm]] for the given mode by trying any user-registered
   * [[SchedulingAlgorithmProvider]]s first and falling back to [[BuiltInAlgorithmProvider]].
   *
   * For built-in modes (FIFO, FAIR) this always returns the corresponding algorithm. For unknown
   * modes that are not handled by any provider, this throws [[IllegalArgumentException]] to
   * surface user misconfiguration eagerly.
   */
  def resolveSchedulingAlgorithm(mode: String, conf: SparkConf): SchedulingAlgorithm = {
    val customProviders = loadCustomProviders(conf)
    val providers = customProviders :+ BuiltInAlgorithmProvider
    providers.view
      .flatMap(_.createAlgorithm(mode, conf))
      .headOption
      .getOrElse {
        throw new IllegalArgumentException(
          s"Unsupported scheduling mode: $mode. Use FAIR or FIFO, or register a custom " +
            s"${classOf[SchedulingAlgorithmProvider].getSimpleName} that supports it via " +
            s"${SCHEDULER_ALGORITHM_PROVIDERS.key}.")
      }
  }

  /**
   * Load user-registered [[SchedulingAlgorithmProvider]]s, rejecting any that attempt to redefine
   * a built-in mode (FIFO/FAIR/NONE). Built-in modes are owned by Spark and must not be overridden.
   */
  def loadCustomProviders(conf: SparkConf): Seq[SchedulingAlgorithmProvider] = {
    val classes = conf.get(SCHEDULER_ALGORITHM_PROVIDERS).filter(_.nonEmpty)
    val providers = Utils.loadExtensions(classOf[SchedulingAlgorithmProvider], classes, conf)
    providers.foreach { provider =>
      val overridden = provider.supportedModes.filter(SchedulingMode.isBuiltIn)
      require(overridden.isEmpty,
        s"${provider.getClass.getName} may not override built-in scheduling " +
          s"mode(s) ${overridden.mkString(", ")}; built-in modes (FIFO, FAIR) are reserved.")
    }
    providers
  }
}

/**
 * A Schedulable entity that represents collection of Pools or TaskSetManagers
 */
private[spark] class Pool(
    val poolName: String,
    val schedulingMode: String,
    initMinShare: Int,
    initWeight: Int)
  extends Schedulable with Logging {

  val schedulableQueue = new ConcurrentLinkedQueue[Schedulable]
  val schedulableNameToSchedulable = new ConcurrentHashMap[String, Schedulable]
  val weight = initWeight
  val minShare = initMinShare
  var runningTasks = 0
  val priority = 0

  // A pool's stage id is used to break the tie in scheduling.
  var stageId = -1
  val name = poolName
  var parent: Pool = null

  private val taskSetSchedulingAlgorithm: SchedulingAlgorithm =
    Pool.resolveSchedulingAlgorithm(schedulingMode, SparkEnv.get.conf)

  override def isSchedulable: Boolean = true

  override def addSchedulable(schedulable: Schedulable): Unit = {
    require(schedulable != null)
    schedulableQueue.add(schedulable)
    schedulableNameToSchedulable.put(schedulable.name, schedulable)
    schedulable.parent = this
  }

  override def removeSchedulable(schedulable: Schedulable): Unit = {
    schedulableQueue.remove(schedulable)
    schedulableNameToSchedulable.remove(schedulable.name)
  }

  override def getSchedulableByName(schedulableName: String): Schedulable = {
    if (schedulableNameToSchedulable.containsKey(schedulableName)) {
      return schedulableNameToSchedulable.get(schedulableName)
    }
    for (schedulable <- schedulableQueue.asScala) {
      val sched = schedulable.getSchedulableByName(schedulableName)
      if (sched != null) {
        return sched
      }
    }
    null
  }

  override def executorLost(executorId: String, host: String, reason: ExecutorLossReason): Unit = {
    schedulableQueue.asScala.foreach(_.executorLost(executorId, host, reason))
  }

  override def executorDecommission(executorId: String): Unit = {
    schedulableQueue.asScala.foreach(_.executorDecommission(executorId))
  }

  override def checkSpeculatableTasks(minTimeToSpeculation: Long): Boolean = {
    var shouldRevive = false
    for (schedulable <- schedulableQueue.asScala) {
      shouldRevive |= schedulable.checkSpeculatableTasks(minTimeToSpeculation)
    }
    shouldRevive
  }

  override def getSortedTaskSetQueue: ArrayBuffer[TaskSetManager] = {
    val sortedTaskSetQueue = new ArrayBuffer[TaskSetManager]
    val sortedSchedulableQueue =
      schedulableQueue.asScala.toSeq.sortWith(taskSetSchedulingAlgorithm.comparator)
    for (schedulable <- sortedSchedulableQueue) {
      sortedTaskSetQueue ++= schedulable.getSortedTaskSetQueue.filter(_.isSchedulable)
    }
    sortedTaskSetQueue
  }

  def increaseRunningTasks(taskNum: Int): Unit = {
    runningTasks += taskNum
    if (parent != null) {
      parent.increaseRunningTasks(taskNum)
    }
  }

  def decreaseRunningTasks(taskNum: Int): Unit = {
    runningTasks -= taskNum
    if (parent != null) {
      parent.decreaseRunningTasks(taskNum)
    }
  }
}
