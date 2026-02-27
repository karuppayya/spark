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

package org.apache.spark.shuffle.vault;

import scala.Option;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkException;
import org.apache.spark.TaskContext;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.scheduler.MapStatus;
import org.apache.spark.scheduler.MapStatus$;
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter;
import org.apache.spark.shuffle.api.ShuffleExecutorComponents;
import org.apache.spark.shuffle.sort.SerializedShuffleHandle;
import org.apache.spark.shuffle.sort.UnsafeShuffleWriter;
import org.apache.spark.storage.BlockManager;
import org.apache.spark.storage.RemoteShuffleStorage;

/**
 * Extends UnsafeShuffleWriter to use RemoteShuffleStorage.BLOCK_MANAGER_ID in MapStatus.
 * All shuffle write logic is inherited from UnsafeShuffleWriter - only the BlockManagerId
 * in the final MapStatus is changed to point to remote storage.
 */
public class RemoteUnsafeShuffleWriter<K, V> extends UnsafeShuffleWriter<K, V> {

  private final long mapId;

  public RemoteUnsafeShuffleWriter(
      BlockManager blockManager,
      TaskMemoryManager memoryManager,
      SerializedShuffleHandle<K, V> handle,
      long mapId,
      TaskContext taskContext,
      SparkConf sparkConf,
      ShuffleWriteMetricsReporter writeMetrics,
      ShuffleExecutorComponents shuffleExecutorComponents) throws SparkException {
    super(blockManager, memoryManager, handle, mapId, taskContext,
        sparkConf, writeMetrics, shuffleExecutorComponents);
    this.mapId = mapId;
  }

  /**
   * Override stop to replace BlockManagerId in MapStatus with remote storage ID.
   */
  @Override
  public Option<MapStatus> stop(boolean success) {
    Option<MapStatus> mapStatusOpt = super.stop(success);

    // Replace BlockManagerId with RemoteShuffleStorage.BLOCK_MANAGER_ID
    if (mapStatusOpt.isDefined()) {
      MapStatus mapStatus = mapStatusOpt.get();
      return Option.apply(MapStatus$.MODULE$.apply(
          RemoteShuffleStorage.BLOCK_MANAGER_ID(),
          getPartitionLengths(),
          mapId,
          mapStatus.checksumValue()));
    } else {
      return mapStatusOpt;
    }
  }
}
