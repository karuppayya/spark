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

package org.apache.spark.storage

import java.io.{BufferedOutputStream, DataOutputStream, File}

import scala.concurrent.Future
import scala.reflect.ClassTag

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FSDataOutputStream, Path}

import org.apache.spark.{SparkConf, SparkEnv, SparkException}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys._
import org.apache.spark.internal.config.{STORAGE_DECOMMISSION_FALLBACK_STORAGE_CLEANUP, STORAGE_DECOMMISSION_FALLBACK_STORAGE_PATH}
import org.apache.spark.network.shuffle.BlockFetchingListener
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.rpc.{RpcAddress, RpcEndpointRef, RpcTimeout}
import org.apache.spark.shuffle.{IndexShuffleBlockResolver, ShuffleBlockInfo}
import org.apache.spark.storage.BlockManagerMessages.RemoveShuffle
import org.apache.spark.storage.RemoteShuffleStorage.{appId, remoteFileSystem, remoteStoragePath}
import org.apache.spark.util.Utils

/**
 * A fallback storage used by storage decommissioners.
 */
private[storage] class RemoteShuffleStorage(conf: SparkConf) extends Logging {
  require(conf.contains("spark.app.id"))
  require(conf.get(STORAGE_DECOMMISSION_FALLBACK_STORAGE_PATH).isDefined)

  // TODO: Introduce new conf

  // Visible for testing
  def copy(shuffleBlockInfo: ShuffleBlockInfo): Unit = {
    val shuffleId = shuffleBlockInfo.shuffleId
    val mapId = shuffleBlockInfo.mapId
    val startTimeNs = System.nanoTime()
    val resolver = SparkEnv.get.shuffleManager.shuffleBlockResolver.
      asInstanceOf[IndexShuffleBlockResolver]
    val indexFile: File = resolver.getIndexFile(shuffleId, mapId)
    val dataFile: File = resolver.getDataFile(shuffleId, mapId)
    val length = dataFile.length
    if (indexFile.exists() && dataFile.exists()) {
      val hash = JavaUtils.nonNegativeHash(dataFile.getName)
      remoteFileSystem.copyFromLocalFile(true,
        new Path(Utils.resolveURI(dataFile.getAbsolutePath)),
        new Path(remoteStoragePath, s"$appId/$shuffleId/$hash/${dataFile.getName}"))
    }
    logWarning(s"Write took ${(System.nanoTime() - startTimeNs) / (1000 * 1000)}ms," +
      s" size: ${Utils.bytesToString(length)}")
  }
}

private[storage] class RemoteStorageRpcEndpointRef(conf: SparkConf) extends RpcEndpointRef(conf) {
  // scalastyle:off executioncontextglobal
  import scala.concurrent.ExecutionContext.Implicits.global
  // scalastyle:on executioncontextglobal
  override def address: RpcAddress = null
  override def name: String = "remoteStorageEndpoint"
  override def send(message: Any): Unit = {}
    override def ask[T: ClassTag](message: Any, timeout: RpcTimeout): Future[T] = {
    message match {
      case RemoveShuffle(shuffleId) =>
        val dataFile = new Path(remoteStoragePath, s"$appId/$shuffleId")
        SparkEnv.get.mapOutputTracker.unregisterShuffle(shuffleId)
        val shuffleManager = SparkEnv.get.shuffleManager
        if (shuffleManager != null) {
          shuffleManager.unregisterShuffle(shuffleId)
        } else {
          logDebug(log"Ignore remove shuffle ${MDC(SHUFFLE_ID, shuffleId)}")
        }
        Future {
          remoteFileSystem.delete(dataFile, true).asInstanceOf[T]
        }
      case _ =>
        Future{true.asInstanceOf[T]}
    }
  }
}


private[spark] object RemoteShuffleStorage extends Logging {

  val shuffleBlockRemoteStorage = new RemoteShuffleStorage(SparkEnv.get.conf)
  val blockManagerId = "remoteShuffleBlockStore"
  lazy val hadoopConf: Configuration = SparkHadoopUtil.get.newConfiguration(SparkEnv.get.conf)
  val appId: String = SparkEnv.get.conf.getAppId
  val remoteStoragePath = new Path(SparkEnv.get.
    conf.get(STORAGE_DECOMMISSION_FALLBACK_STORAGE_PATH).get)
  val remoteFileSystem: FileSystem = FileSystem.get(remoteStoragePath.toUri, hadoopConf)

  /** We use one block manager id as a place holder. */
  val BLOCK_MANAGER_ID: BlockManagerId = BlockManagerId(blockManagerId, "remote", 7337)

  /** Register the remote shuffle block manager and its RPC endpoint. */
  def registerBlockManager(master: BlockManagerMaster, conf: SparkConf): Unit = {
    master.registerBlockManager(
      BLOCK_MANAGER_ID, Array.empty[String], 0, 0, new RemoteStorageRpcEndpointRef(conf))
  }

  /** Clean up the generated fallback location for this app. */
  def cleanUp(conf: SparkConf, hadoopConf: Configuration): Unit = {
    if (conf.get(STORAGE_DECOMMISSION_FALLBACK_STORAGE_PATH).isDefined &&
        conf.get(STORAGE_DECOMMISSION_FALLBACK_STORAGE_CLEANUP) &&
        conf.contains("spark.app.id")) {
      val fallbackPath =
        new Path(conf.get(STORAGE_DECOMMISSION_FALLBACK_STORAGE_PATH).get, conf.getAppId)
      val fallbackUri = fallbackPath.toUri
      val remoteFileSystem = FileSystem.get(fallbackUri, hadoopConf)
      // The fallback directory for this app may not be created yet.
      if (remoteFileSystem.exists(fallbackPath)) {
        if (remoteFileSystem.delete(fallbackPath, true)) {
          logInfo(log"Succeed to clean up: ${MDC(URI, fallbackUri)}")
        } else {
          // Clean-up can fail due to the permission issues.
          logWarning(log"Failed to clean up: ${MDC(URI, fallbackUri)}")
        }
      }
    }
  }

  /**
   * Read a ManagedBuffer.
   */
  def read(blockIds: Seq[BlockId], listener: BlockFetchingListener): Unit = {
    blockIds.foreach(blockId => {
      logInfo(log"Read ${MDC(BLOCK_ID, blockId)}")
      listener.onBlockFetchSuccess(blockId.name,
        new FileSystemManagedBuffer(getPath(blockId), hadoopConf))
    })
  }

  def getPath(blockId: BlockId): Path = {
    val (shuffleId, _) = blockId match {
      case ShuffleBlockId(shuffleId, mapId, _) =>
        (shuffleId, mapId)
      case ShuffleDataBlockId(shuffleId, mapId, _) =>
        (shuffleId, mapId)
      case ShuffleIndexBlockId(shuffleId, mapId, _) =>
        (shuffleId, mapId)
      case ShuffleChecksumBlockId(shuffleId, mapId, _) =>
        (shuffleId, mapId)
      case _ => (0, 0.toLong)
    }
      blockId match {
        case ShuffleDataBlockId(_, _, _) =>
        case ShuffleIndexBlockId(_, _, _) =>
        case ShuffleChecksumBlockId(_, _, _) =>
        case _ => throw new SparkException(s"Unsupported block id type: ${blockId.name}")
      }
      val hash = JavaUtils.nonNegativeHash(blockId.name)
      new Path(remoteStoragePath, s"$appId/$shuffleId/$hash/${blockId.name}")
  }

  def getStream(blockId: BlockId): FSDataOutputStream = {
    val path = getPath(blockId)
    remoteFileSystem.create(path)
  }

  def writeCheckSum(blockId: BlockId, array: Array[Long]): Unit = {
    val out = new DataOutputStream(new BufferedOutputStream(getStream(blockId),
      scala.math.min(8192, 8 * array.length)))
    array.foreach(out.writeLong)
    out.flush()
    out.close()
  }

}

