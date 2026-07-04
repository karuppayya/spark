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

package org.apache.spark.scheduler

import java.util.Locale

import org.apache.spark.annotation.DeveloperApi

/**
 * :: DeveloperApi ::
 *  "FAIR" and "FIFO" determines which policy is used
 *    to order tasks amongst a Schedulable's sub-queues
 *  "NONE" is used when the a Schedulable has no sub-queues.
 *
 * These are the built-in scheduling modes. Additional modes may be introduced at runtime by
 * registering a [[SchedulingAlgorithmProvider]]; such modes are represented as plain mode name
 * strings rather than values of this enumeration.
 */
@DeveloperApi
object SchedulingMode extends Enumeration {

  type SchedulingMode = Value
  val FAIR, FIFO, NONE = Value

  /** Mode names reserved by Spark; custom providers may not redefine these. */
  val BUILT_IN_MODES: Set[String] = values.map(_.toString)

  def isBuiltIn(mode: String): Boolean =
    BUILT_IN_MODES.contains(mode.toUpperCase(Locale.ROOT))
}
