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

import org.apache.spark.ShuffleDependency
import org.apache.spark.shuffle.ShuffleHandle

/**
 * Marker ShuffleHandle to indicate this shuffle should use remote storage (S3/ShuffleVault).
 * This handle wraps the underlying shuffle handle from the base shuffle manager.
 *
 * When a ShuffleVaultManager returns this handle type, shuffle writers and readers
 * can detect it and route shuffle data to/from remote storage instead of local disk.
 *
 * @param shuffleId The shuffle ID
 * @param dependency The shuffle dependency
 * @param wrappedHandle The underlying shuffle handle from the delegated shuffle manager
 */
private[spark] class ShuffleVaultHandle[K, V, C](
    shuffleId: Int,
    val dependency: ShuffleDependency[K, V, C],
    val wrappedHandle: ShuffleHandle)
  extends ShuffleHandle(shuffleId)
