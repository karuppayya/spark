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

import org.apache.spark.SparkEnv
import org.apache.spark.api.plugin.{DriverPlugin, ExecutorPlugin, PluginContext, SparkPlugin}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.storage.RemoteShuffleStorage

/**
 * SparkPlugin that registers the remote shuffle block manager and handles cleanup.
 *
 * This replaces direct modifications to SparkContext by using the plugin lifecycle:
 * - registerMetrics: called after blockManager.initialize(), registers the remote block manager
 * - shutdown: called during driver shutdown, cleans up remote storage
 *
 * Configure via:
 * {{{
 *   spark.plugins=org.apache.spark.shuffle.vault.ShuffleVaultPlugin
 * }}}
 */
class ShuffleVaultPlugin extends SparkPlugin {
  override def driverPlugin(): DriverPlugin = new ShuffleVaultDriverPlugin()
  override def executorPlugin(): ExecutorPlugin = null
}

class ShuffleVaultDriverPlugin extends DriverPlugin {

  /**
   * Called after blockManager.initialize() - safe to register the remote shuffle block manager.
   */
  override def registerMetrics(appId: String, pluginContext: PluginContext): Unit = {
    val conf = pluginContext.conf()
    val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)
    val blockManagerMaster = SparkEnv.get.blockManager.master
    RemoteShuffleStorage.registerBlockManagerifNeeded(blockManagerMaster, conf, hadoopConf)
  }

  /**
   * Called during driver shutdown - clean up remote shuffle storage.
   */
  override def shutdown(): Unit = {
    val conf = SparkEnv.get.conf
    val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)
    RemoteShuffleStorage.cleanUp(conf, hadoopConf)
  }
}
