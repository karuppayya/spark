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

import org.apache.spark.SparkEnv;
import org.apache.spark.shuffle.api.ShuffleDriverComponents;
import org.apache.spark.shuffle.sort.io.LocalDiskShuffleDriverComponents;
import org.apache.spark.storage.BlockManagerMaster;

import java.util.Collections;
import java.util.Map;

public class HybridShuffleDriverComponents implements ShuffleDriverComponents {

  public HybridShuffleDriverComponents(LocalDiskShuffleDriverComponents
      localDiskShuffleDriverComponents) {
    this.localDiskShuffleDriverComponents = localDiskShuffleDriverComponents;
  }

  LocalDiskShuffleDriverComponents localDiskShuffleDriverComponents;

  private BlockManagerMaster blockManagerMaster;

  @Override
  public Map<String, String> initializeApplication() {
    blockManagerMaster = SparkEnv.get().blockManager().master();
    return Collections.emptyMap();
  }

  @Override
  public void cleanupApplication() {
    // TODO cleanup remote storage
  }

  @Override
  public void removeShuffle(int shuffleId, boolean blocking) {
    localDiskShuffleDriverComponents.removeShuffle(shuffleId, blocking);
    // TODO remove from remote shuffle storage
  }
}
