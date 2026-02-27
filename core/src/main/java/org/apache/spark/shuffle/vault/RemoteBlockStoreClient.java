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

import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.shuffle.BlockFetchingListener;
import org.apache.spark.network.shuffle.BlockStoreClient;
import org.apache.spark.network.shuffle.DownloadFileManager;
import org.apache.spark.storage.BlockId;
import org.apache.spark.storage.FileSystemManagedBuffer;
import org.apache.spark.storage.RemoteShuffleStorage;

/**
 * A BlockStoreClient that reads shuffle blocks directly from remote storage (e.g., S3/HDFS)
 * via RemoteShuffleStorage.
 *
 * Used exclusively by RemoteShuffleReader — all blocks are expected to be remote.
 * This allows ShuffleBlockFetcherIterator to handle remote shuffle blocks transparently,
 * preserving its prefetching and pipelining behavior.
 */
public class RemoteBlockStoreClient extends BlockStoreClient {

  public RemoteBlockStoreClient() {}

  @Override
  public void fetchBlocks(
      String host,
      int port,
      String execId,
      String[] blockIds,
      BlockFetchingListener listener,
      DownloadFileManager downloadFileManager) {

    for (String blockIdStr : blockIds) {
      try {
        BlockId blockId = BlockId.apply(blockIdStr);
        ManagedBuffer buf = new FileSystemManagedBuffer(
            RemoteShuffleStorage.getPath(blockId),
            RemoteShuffleStorage.hadoopConf(),
            64);
        listener.onBlockFetchSuccess(blockIdStr, buf);
      } catch (Exception e) {
        listener.onBlockFetchFailure(blockIdStr, e);
      }
    }
  }

  @Override
  public void close() {}
}
