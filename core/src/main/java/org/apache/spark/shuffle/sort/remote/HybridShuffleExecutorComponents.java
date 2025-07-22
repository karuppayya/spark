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

package org.apache.spark.shuffle.sort.remote;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.shuffle.api.ShuffleExecutorComponents;
import org.apache.spark.shuffle.api.ShuffleMapOutputWriter;
import org.apache.spark.shuffle.api.SingleSpillShuffleMapOutputWriter;
import org.apache.spark.shuffle.sort.io.LocalDiskShuffleExecutorComponents;
import org.apache.spark.storage.BlockManager;

import java.util.Map;
import java.util.Optional;

public class HybridShuffleExecutorComponents implements ShuffleExecutorComponents {

  private final SparkConf sparkConf;
  private BlockManager blockManager;

  private final LocalDiskShuffleExecutorComponents localDiskShuffleExecutorComponents;

  public HybridShuffleExecutorComponents(SparkConf sparkConf,
       LocalDiskShuffleExecutorComponents localDiskShuffleExecutorComponents) {
    this.sparkConf = sparkConf;
    this.localDiskShuffleExecutorComponents = localDiskShuffleExecutorComponents;
  }

  @Override
  public void initializeExecutor(String appId, String execId, Map<String, String> extraConfigs) {
    blockManager = SparkEnv.get().blockManager();
    if (blockManager == null) {
      throw new IllegalStateException("No blockManager available from the SparkEnv.");
    }
  }

  @Override
  public ShuffleMapOutputWriter createMapOutputWriter(
      int shuffleId,
      long mapTaskId,
      int numPartitions) {
    boolean isLocal = false;
    if (isLocal) {
      return localDiskShuffleExecutorComponents.createMapOutputWriter(shuffleId, mapTaskId,
              numPartitions);
    } else {
      return new S3ShuffleMapOutputWriter(sparkConf, shuffleId, mapTaskId, numPartitions);
    }
  }

  @Override
  public Optional<SingleSpillShuffleMapOutputWriter> createSingleFileMapOutputWriter(
      int shuffleId,
      long mapId) {
    return Optional.empty();
  }
}
