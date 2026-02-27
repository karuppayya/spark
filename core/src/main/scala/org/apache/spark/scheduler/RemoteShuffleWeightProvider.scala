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

import org.apache.spark.SparkEnv
import org.apache.spark.shuffle.vault.ShuffleVaultManager

/**
 * Weight provider that assigns higher priority to remote shuffle stages.
 *
 * Remote shuffle stages are identified by querying the ShuffleVaultManager to check
 * if the shuffle is registered for remote storage (e.g., S3). This works for stages
 * that read shuffle data from remote storage.
 *
 * When a TaskSet is identified as reading from a remote shuffle, it receives a weight of 1000,
 * otherwise it receives a weight of 1.
 *
 * This allows remote shuffle stages to be prioritized over local shuffle stages,
 * improving overall job performance by starting remote I/O operations earlier.
 *
 * Note: This only works when spark.shuffle.manager is set to ShuffleVaultManager.
 *
 * Usage:
 * {{{
 *   spark.scheduler.taskset.weight.provider.class=
 *     org.apache.spark.scheduler.RemoteShuffleWeightProvider
 * }}}
 */
private[spark] class RemoteShuffleWeightProvider extends TaskSetWeightProvider {

  private val REMOTE_STAGE_WEIGHT = 1000
  private val DEFAULT_STAGE_WEIGHT = 1

  override def getWeight(taskSetManager: TaskSetManager): Int = {
    // Check if this TaskSet is for shuffle map tasks or has a shuffle dependency
    val taskSet = taskSetManager.taskSet
    val shuffleIdOpt = taskSet.shuffleId

    shuffleIdOpt match {
      case Some(shuffleId) =>
        // Check if the shuffle manager is ShuffleVaultManager and if this shuffle is remote
        val shuffleManager = SparkEnv.get.shuffleManager
        shuffleManager match {
          case vaultManager: ShuffleVaultManager =>
            if (vaultManager.isRemoteShuffle(shuffleId)) {
              REMOTE_STAGE_WEIGHT
            } else {
              DEFAULT_STAGE_WEIGHT
            }
          case _ =>
            // Not using ShuffleVaultManager, all shuffles are local
            DEFAULT_STAGE_WEIGHT
        }
      case None =>
        // No shuffle ID, this is not a shuffle stage
        DEFAULT_STAGE_WEIGHT
    }
  }
}
