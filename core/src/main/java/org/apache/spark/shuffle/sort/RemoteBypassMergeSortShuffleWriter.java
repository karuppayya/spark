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

package org.apache.spark.shuffle.sort;

import java.io.IOException;

import scala.Option;
import scala.Product2;
import scala.collection.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkException;
import org.apache.spark.scheduler.MapStatus;
import org.apache.spark.scheduler.MapStatus$;
import org.apache.spark.shuffle.ShuffleWriter;
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter;
import org.apache.spark.shuffle.api.ShuffleExecutorComponents;
import org.apache.spark.storage.BlockManager;
import org.apache.spark.storage.RemoteShuffleStorage;

/**
 * Wrapper around BypassMergeSortShuffleWriter that replaces the BlockManagerId in MapStatus
 * with RemoteShuffleStorage.BLOCK_MANAGER_ID for remote shuffles.
 */
public class RemoteBypassMergeSortShuffleWriter<K, V> extends ShuffleWriter<K, V> {

  private final BypassMergeSortShuffleWriter<K, V> underlying;
  private final long mapId;

  public RemoteBypassMergeSortShuffleWriter(
      BlockManager blockManager,
      BypassMergeSortShuffleHandle<K, V> handle,
      long mapId,
      SparkConf conf,
      ShuffleWriteMetricsReporter writeMetrics,
      ShuffleExecutorComponents shuffleExecutorComponents) throws SparkException {
    this.underlying = new BypassMergeSortShuffleWriter<>(
        blockManager, handle, mapId, conf, writeMetrics, shuffleExecutorComponents);
    this.mapId = mapId;
  }

  @Override
  public void write(Iterator<Product2<K, V>> records) throws IOException {
    underlying.write(records);
  }

  @Override
  public Option<MapStatus> stop(boolean success) {
    Option<MapStatus> mapStatusOpt = underlying.stop(success);

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

  @Override
  public long[] getPartitionLengths() {
    return underlying.getPartitionLengths();
  }
}
