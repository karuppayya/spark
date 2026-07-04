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

import java.util.Properties

import org.apache.spark.annotation.DeveloperApi

/**
 * :: DeveloperApi ::
 * Read-only snapshot of a TaskSet's scheduling-relevant fields, passed to
 * [[TaskSetWeightProvider]] implementations. This is a stable view object so that the internal
 * `TaskSetManager` type does not appear in the public API.
 *
 * @param stageId         id of the stage this TaskSet belongs to
 * @param stageAttemptId  stage attempt id
 * @param priority        job priority (lower is higher priority)
 * @param numTasks        number of tasks in the TaskSet
 * @param resourceProfileId resource profile id used by the TaskSet
 * @param properties      job-local properties (e.g. `spark.scheduler.pool`); may be `null`
 *
 * @since 5.0.0
 */
@DeveloperApi
final case class TaskSetInfo(
    stageId: Int,
    stageAttemptId: Int,
    priority: Int,
    numTasks: Int,
    resourceProfileId: Int,
    properties: Properties)

/**
 * :: DeveloperApi ::
 * Trait for providing scheduling weights for TaskSets.*
 */
@DeveloperApi
trait TaskSetWeightProvider {
  /**
   * Calculate the scheduling weight for a TaskSet.
   */
  def getWeight(taskSet: TaskSetInfo): Int
}

private[spark] object DefaultWeightProvider extends TaskSetWeightProvider {
  override def getWeight(taskSet: TaskSetInfo): Int = 1
}
