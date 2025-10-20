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

package org.apache.spark.shuffle

import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.ShuffleDependency
import org.apache.spark.internal.Logging

/**
 * A registry that stores ShuffleDependency objects by their shuffleId.
 *
 * The registry is populated when ShuffleDependency objects are deserialized
 * in ShuffleMapTask.runTask().
 */
private[spark] object ShuffleDependencyRegistry extends Logging {

  // Thread-safe map to store shuffleId -> ShuffleDependency mapping
  private val shuffleDependencyMap = new ConcurrentHashMap[Int, ShuffleDependency[_, _, _]]()

  /**
   * Register a ShuffleDependency in the registry.
   *
   * @param dependency the ShuffleDependency to register
   */
  def registerShuffleDependency(dependency: ShuffleDependency[_, _, _]): Unit = {
    shuffleDependencyMap.put(dependency.shuffleId, dependency)
  }

  /**
   * Get a ShuffleDependency by shuffleId.
   *
   * @param shuffleId the shuffle ID
   * @return Some(ShuffleDependency) if found, None otherwise
   */
  def getShuffleDependency(shuffleId: Int): Option[ShuffleDependency[_, _, _]] = {
    Option(shuffleDependencyMap.get(shuffleId))
  }

  /**
   * Remove a ShuffleDependency from the registry.
   *
   * @param shuffleId the shuffle ID to remove
   * @return true if the entry was removed, false if it didn't exist
   */
  def unregisterShuffleDependency(shuffleId: Int): Boolean = {
    shuffleDependencyMap.remove(shuffleId) != null
  }

  /**
   * Get the current number of entries in the registry.
   *
   * @return the number of entries
   */
  def size: Int = shuffleDependencyMap.size()

  /**
   * Check if the registry is empty.
   *
   * @return true if empty, false otherwise
   */
  def isEmpty: Boolean = shuffleDependencyMap.isEmpty

  /**
   * Clear all entries from the registry.
   */
  def clear(): Unit = {
    shuffleDependencyMap.clear()
    logInfo("Cleared all shuffle dependencies from registry")
  }
}
