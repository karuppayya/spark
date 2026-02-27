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

package org.apache.spark.sql.execution.exchange

import scala.concurrent.Future

import org.apache.spark.{MapOutputStatistics, PartitionIdPassthrough, ShuffleDependency, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.logical.Statistics
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics, SQLShuffleReadMetricsReporter, SQLShuffleWriteMetricsReporter}
import org.apache.spark.util.MutablePair

/**
 * A shuffle exchange that consolidates shuffle data from earlier stages by re-shuffling
 * with partition-ID pass-through semantics. Each row stays in the same partition it came from,
 * but the data is written through the shuffle write path (enabling remote storage upload).
 *
 * This is a self-contained exchange that does not modify stock Spark's ShuffleExchangeExec.
 * It handles its own partitioner and key extractor internally.
 */
case class ConsolidationShuffleExchangeExec(
    child: SparkPlan,
    originalPartitioning: Partitioning,
    advisoryPartitionSize: Option[Long] = None)
  extends ShuffleExchangeLike {

  override val outputPartitioning: Partitioning = originalPartitioning

  override def shuffleOrigin: ShuffleOrigin = REQUIRED_BY_STATEFUL_OPERATOR

  override def nodeName: String = "Consolidation exchange"

  private lazy val writeMetrics =
    SQLShuffleWriteMetricsReporter.createShuffleWriteMetrics(sparkContext)
  private[sql] lazy val readMetrics =
    SQLShuffleReadMetricsReporter.createShuffleReadMetrics(sparkContext)
  override lazy val metrics: Map[String, SQLMetric] = Map(
    "dataSize" -> SQLMetrics.createSizeMetric(sparkContext, "data size"),
    "numPartitions" -> SQLMetrics.createMetric(sparkContext, "number of partitions")
  ) ++ readMetrics ++ writeMetrics

  private lazy val serializer: Serializer =
    new UnsafeRowSerializer(child.output.size, longMetric("dataSize"))

  @transient lazy val inputRDD: RDD[InternalRow] = child.execute()

  @transient
  override lazy val mapOutputStatisticsFuture: Future[MapOutputStatistics] = {
    if (inputRDD.getNumPartitions == 0) {
      Future.successful(null)
    } else {
      sparkContext.submitMapStage(shuffleDependency)
    }
  }

  override def numMappers: Int = shuffleDependency.rdd.getNumPartitions

  override def numPartitions: Int = shuffleDependency.partitioner.numPartitions

  override def getShuffleRDD(partitionSpecs: Array[ShufflePartitionSpec]): RDD[InternalRow] = {
    new ShuffledRowRDD(shuffleDependency, readMetrics, partitionSpecs)
  }

  override def runtimeStatistics: Statistics = {
    val dataSize = metrics("dataSize").value
    val rowCount = metrics(SQLShuffleWriteMetricsReporter.SHUFFLE_RECORDS_WRITTEN).value
    Statistics(dataSize, Some(rowCount))
  }

  override def shuffleId: Int = shuffleDependency.shuffleId

  @transient
  lazy val shuffleDependency: ShuffleDependency[Int, InternalRow, InternalRow] = {
    val numParts = originalPartitioning.numPartitions

    // Partition-ID pass-through: each row stays in its original partition
    val rddWithPartitionIds: RDD[Product2[Int, InternalRow]] = {
      inputRDD.mapPartitionsWithIndexInternal((_, iter) => {
        val partitionId = TaskContext.getPartitionId()
        val mutablePair = new MutablePair[Int, InternalRow]()
        iter.map { row => mutablePair.update(partitionId, row) }
      }, isOrderSensitive = false)
    }

    val dep = new ShuffleDependency[Int, InternalRow, InternalRow](
      rddWithPartitionIds,
      new PartitionIdPassthrough(numParts),
      serializer,
      shuffleWriterProcessor = ShuffleExchangeExec.createShuffleWriteProcessor(writeMetrics))

    metrics("numPartitions").set(dep.partitioner.numPartitions)
    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    SQLMetrics.postDriverMetricUpdates(
      sparkContext, executionId, metrics("numPartitions") :: Nil)
    dep
  }

  protected override def doExecute(): RDD[InternalRow] = {
    new ShuffledRowRDD(shuffleDependency, readMetrics)
  }

  override protected def withNewChildInternal(
      newChild: SparkPlan): ConsolidationShuffleExchangeExec =
    copy(child = newChild)
}
