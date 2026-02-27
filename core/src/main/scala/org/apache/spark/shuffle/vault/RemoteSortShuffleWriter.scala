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

package org.apache.spark.shuffle.vault

import org.apache.spark.TaskContext
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.BaseShuffleHandle
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter
import org.apache.spark.shuffle.api.ShuffleExecutorComponents
import org.apache.spark.shuffle.sort.SortShuffleWriter
import org.apache.spark.storage.RemoteShuffleStorage

/**
 * Extends SortShuffleWriter to use RemoteShuffleStorage.BLOCK_MANAGER_ID in MapStatus.
 * All shuffle write logic is inherited from SortShuffleWriter - only the BlockManagerId
 * in the final MapStatus is changed to point to remote storage.
 */
private[spark] class RemoteSortShuffleWriter[K, V, C](
    handle: BaseShuffleHandle[K, V, C],
    mapId: Long,
    context: TaskContext,
    writeMetrics: ShuffleWriteMetricsReporter,
    shuffleExecutorComponents: ShuffleExecutorComponents)
  extends SortShuffleWriter[K, V, C](
    handle, mapId, context, writeMetrics, shuffleExecutorComponents) {

  /**
   * Override stop to replace BlockManagerId in MapStatus with remote storage ID.
   */
  override def stop(success: Boolean): Option[MapStatus] = {
    val mapStatusOpt = super.stop(success)

    // Replace BlockManagerId with RemoteShuffleStorage.BLOCK_MANAGER_ID
    mapStatusOpt.map { mapStatus =>
      MapStatus(
        RemoteShuffleStorage.BLOCK_MANAGER_ID,
        getPartitionLengths(),
        mapStatus.mapId,
        mapStatus.checksumValue)
    }
  }
}
