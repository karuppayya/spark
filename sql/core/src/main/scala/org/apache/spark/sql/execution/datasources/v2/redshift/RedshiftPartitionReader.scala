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
package org.apache.spark.sql.execution.datasources.v2.redshift

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.{JobID, TaskAttemptID, TaskID, TaskType}
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

class RedshiftPartitionReader(path: String,
    start: Long,
    length: Long,
    locations: Array[String],
    schema: StructType,
    conf: Broadcast[SerializableConfiguration]) extends PartitionReader[InternalRow] {

  new FileSplit(new Path(path), start, length, locations)

  val attemptId = new TaskAttemptID(new TaskID(new JobID(), TaskType.MAP, 0), 0)
  val hadoopAttemptContext = new TaskAttemptContextImpl(conf.value.value, attemptId)
  val recordReader = new RedshiftRecordReader
  recordReader.initialize(new FileSplit(new Path(path), start, length, locations),
    hadoopAttemptContext)
  val iter = new RecordReaderIterator[Array[String]](recordReader)
  val converter = Conversions.createRowConverter(schema,
    Parameters.DEFAULT_PARAMETERS("csvnullstring"))

  override def next(): Boolean = {
    iter.hasNext
  }

  override def get(): InternalRow = {
    converter(iter.next())
  }

  override def close(): Unit = {
    iter.close()
  }
}
