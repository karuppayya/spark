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

import scala.collection.JavaConverters._

import org.apache.hadoop.fs.FileStatus

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.execution.datasources.{FileFormat, FileStatusCache, InMemoryFileIndex}
import org.apache.spark.sql.execution.datasources.v2.FileTable
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

case class RedshiftTable(tableName: String,
    spark: SparkSession,
    options: CaseInsensitiveStringMap,
    JDBCWrapper: JDBCWrapper,
    userSpecifiedSchema: Option[StructType],
    fallbackFileFormat: Class[_ <: FileFormat])
  extends FileTable(spark, options, Seq.empty, userSpecifiedSchema) {

  val params = Parameters.mergeParameters(options.asScala.toMap)

  /**
   * When possible, this method should return the schema of the given `files`.  When the format
   * does not support inference, or no valid files are given should return None.  In these cases
   * Spark will require that user specify the schema manually.
   */
  override def inferSchema(files: Seq[FileStatus]): Option[StructType] = {
    userSpecifiedSchema
  }

  /**
   * The string that represents the format that this data source provider uses. This is
   * overridden by children to provide a nice alias for the data source. For example:
   *
   * {{{
   *   override def formatName(): String = "ORC"
   * }}}
   */
  override def formatName: String = {
    "redshift"
  }


  /**
   * Returns a {@link ScanBuilder} which can be used to build a {@link Scan}. Spark will call this
   * method to configure each data source scan.
   *
   * @param options The options for reading, which is an immutable case-insensitive
   *                string-to-string map.
   */
  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    val fileStatusCache = FileStatusCache.getOrCreate(spark)
    val index = new InMemoryFileIndex(
      spark, Seq.empty, params.parameters, userSpecifiedSchema, fileStatusCache)

    RedshiftScanBuilder(spark, index, schema, dataSchema, params)
  }

  /**
   * Returns a {@link WriteBuilder} which can be used to create {@link BatchWrite}. Spark will call
   * this method to configure each data source write.
   */
  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    return null
  }

  /**
   * A name to identify this table. Implementations should provide a meaningful name, like the
   * database and table name from catalog, or the location of files for this table.
   */
  override def name(): String = "redshift"

}
