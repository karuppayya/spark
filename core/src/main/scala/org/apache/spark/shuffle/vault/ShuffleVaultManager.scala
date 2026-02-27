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

import java.util.concurrent.ConcurrentHashMap

import scala.jdk.CollectionConverters._

import org.apache.spark.{ShuffleDependency, SparkConf, SparkEnv, TaskContext}
import org.apache.spark.shuffle._
import org.apache.spark.shuffle.api.ShuffleExecutorComponents
import org.apache.spark.shuffle.sort.{RemoteBypassMergeSortShuffleWriter, SortShuffleManager}

/**
 * Pluggable ShuffleManager that enables remote shuffle storage (ShuffleVault) support.
 *
 * This manager wraps the standard SortShuffleManager and intercepts shuffle registration
 * to return ShuffleVaultHandle when remote storage is requested. This allows shuffle
 * data to be written to/read from S3 or other remote storage instead of local disk.
 *
 * To enable this shuffle manager:
 * {{{
 *   spark.shuffle.manager = org.apache.spark.shuffle.vault.ShuffleVaultManager
 * }}}
 *
 * The manager automatically infers whether to use remote storage by checking the
 * RDD operation scope. When SQL uses PassThroughPartitioning for shuffle
 * consolidation, the RDD scope name contains "Consolidation exchange", which
 * indicates remote storage should be used.
 *
 * @param conf SparkConf configuration
 */
private[spark] class ShuffleVaultManager(conf: SparkConf) extends ShuffleManager {

  // Delegate to the standard sort shuffle manager
  private val sortShuffleManager = new SortShuffleManager(conf)

  // Load shuffle executor components for creating writers
  private lazy val shuffleExecutorComponents = loadShuffleExecutorComponents(conf)

  // Track which shuffles are using remote storage
  private val remoteShuffleIds = ConcurrentHashMap.newKeySet[Int]()

  /**
   * Register a shuffle and return an appropriate handle.
   * Returns ShuffleVaultHandle if remote storage is requested, otherwise delegates.
   *
   * Remote storage is inferred from the RDD scope:
   * - If the RDD's operation scope name contains "Consolidation exchange", use remote storage
   * - This correctly distinguishes PassThroughPartitioning from HashPartitioning
   */
  override def registerShuffle[K, V, C](
      shuffleId: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {

    val baseHandle = sortShuffleManager.registerShuffle(shuffleId, dependency)

    // Check RDD scope to determine if this is a consolidation shuffle
    val shouldUseRemoteStorage = dependency.rdd.scope
      .exists(_.name.contains("Consolidation exchange"))

    if (shouldUseRemoteStorage) {
      remoteShuffleIds.add(shuffleId)
      new ShuffleVaultHandle(shuffleId, dependency, baseHandle)
    } else {
      baseHandle
    }
  }

  /**
   * Check if a shuffle is using remote storage.
   * @param shuffleId The shuffle ID to check
   * @return true if the shuffle uses remote storage (S3), false otherwise
   */
  def isRemoteShuffle(shuffleId: Int): Boolean = {
    remoteShuffleIds.contains(shuffleId)
  }

  /**
   * Get a shuffle writer for the given handle.
   * For remote shuffles (ShuffleVaultHandle), returns wrapper writers that use
   * RemoteShuffleStorage.BLOCK_MANAGER_ID. For local shuffles, delegates to SortShuffleManager.
   */
  override def getWriter[K, V](
      handle: ShuffleHandle,
      mapId: Long,
      context: TaskContext,
      metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] = {

    handle match {
      case vaultHandle: ShuffleVaultHandle[K @unchecked, V @unchecked, _] =>
        // Remote shuffle - use wrapper writers that set remote BlockManagerId
        // Signal HybridShuffleExecutorComponents to use RemoteShuffleMapOutputWriter
        context.getLocalProperties.setProperty("consolidation.write", "true")
        val wrappedHandle = vaultHandle.wrappedHandle
        val env = SparkEnv.get

        wrappedHandle match {
          case unsafeHandle: org.apache.spark.shuffle.sort.SerializedShuffleHandle[
              K @unchecked, V @unchecked] =>
            new RemoteUnsafeShuffleWriter(
              env.blockManager,
              context.taskMemoryManager(),
              unsafeHandle,
              mapId,
              context,
              env.conf,
              metrics,
              shuffleExecutorComponents).asInstanceOf[ShuffleWriter[K, V]]

          case bypassHandle: org.apache.spark.shuffle.sort.BypassMergeSortShuffleHandle[
              K @unchecked, V @unchecked] =>
            new RemoteBypassMergeSortShuffleWriter(
              env.blockManager,
              bypassHandle,
              mapId,
              env.conf,
              metrics,
              shuffleExecutorComponents).asInstanceOf[ShuffleWriter[K, V]]

          case baseHandle: org.apache.spark.shuffle.BaseShuffleHandle[
              K @unchecked, V @unchecked, _] =>
            new RemoteSortShuffleWriter(
              baseHandle,
              mapId,
              context,
              metrics,
              shuffleExecutorComponents).asInstanceOf[ShuffleWriter[K, V]]
        }

      case _ =>
        // Local shuffle - use native writers
        sortShuffleManager.getWriter(handle, mapId, context, metrics)
    }
  }

  /**
   * Get a shuffle reader for the given handle and partition range.
   * For remote shuffles (ShuffleVaultHandle), returns a RemoteShuffleReader that reads
   * directly from remote storage. For local shuffles, delegates to SortShuffleManager.
   */
  override def getReader[K, C](
      handle: ShuffleHandle,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {

    handle match {
      case vaultHandle: ShuffleVaultHandle[K @unchecked, _, C @unchecked] =>
        // Get block locations from MapOutputTracker
        val blocksByAddress = SparkEnv.get.mapOutputTracker.getMapSizesByExecutorId(
          vaultHandle.shuffleId, startMapIndex, endMapIndex, startPartition, endPartition)
        // Read directly from remote storage, bypassing ShuffleBlockFetcherIterator
        new RemoteShuffleReader(
          vaultHandle.wrappedHandle.asInstanceOf[BaseShuffleHandle[K, _, C]],
          blocksByAddress,
          context,
          metrics)
      case _ =>
        sortShuffleManager.getReader(handle, startMapIndex, endMapIndex,
          startPartition, endPartition, context, metrics)
    }
  }

  /**
   * Unregister a shuffle - delegates to base manager and removes from tracking.
   */
  override def unregisterShuffle(shuffleId: Int): Boolean = {
    remoteShuffleIds.remove(shuffleId)
    sortShuffleManager.unregisterShuffle(shuffleId)
  }

  /**
   * Return the shuffle block resolver - delegates to base manager.
   */
  override def shuffleBlockResolver: ShuffleBlockResolver = {
    sortShuffleManager.shuffleBlockResolver
  }

  /**
   * Shut down the shuffle manager - delegates to base manager.
   */
  override def stop(): Unit = {
    sortShuffleManager.stop()
  }

  /**
   * Load shuffle executor components for creating writers.
   */
  private def loadShuffleExecutorComponents(conf: SparkConf): ShuffleExecutorComponents = {
    val executorComponents = ShuffleDataIOUtils.loadShuffleDataIO(conf).executor()
    val extraConfigs = conf.getAllWithPrefix(ShuffleDataIOUtils.SHUFFLE_SPARK_CONF_PREFIX)
      .toMap
    executorComponents.initializeExecutor(
      conf.getAppId,
      SparkEnv.get.executorId,
      extraConfigs.asJava)
    executorComponents
  }
}
