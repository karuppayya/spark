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

// scalastyle:off println

package org.apache.spark.examples

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.LongType

object LargeJoinBenchmark {

  /**
   * Generates a large DataFrame with randomized data and writes it to a Parquet file.
   *
   * @param spark The active SparkSession.
   * @param tableName The name of the table (e.g., "table_a").
   * @param numRows The number of rows to generate.
   * @param basePath The base directory to write the Parquet files to.
   * @param idOffset An offset to apply to the join key, to control overlap.
   */
  private def createAndWriteTable(spark: SparkSession, tableName: String, numRows: Long,
                                  basePath: String, idOffset: Long = 0L): Unit = {
    println(s"--- Generating data for $tableName ---")

    // Path for the table's data
    val outputPath = s"$basePath/$tableName"

    // 1. Start with a distributed range of numbers. This is the most efficient
    //    way to create a large, partitioned DataFrame.
    //    The number of partitions is crucial for parallelism. We'll set it high.
    val numPartitions = 40
    val df = spark.range(0, numRows, 1, numPartitions).repartition(numPartitions)
    // Introduce a random column and repartition by it to truly randomize the data distribution
    // before generating other columns. This forces a full shuffle.
    val randomizedDf = df
      .withColumn("random_repartition_key", rand()) // Add a random column
      .repartition(numPartitions, col("random_repartition_key"))
      .drop("random_repartition_key") // Drop the helper column if not needed later

    // 2. Generate the columns with random data.
    //    - We create a common 'join_key' for the join operation later.
    val dfWithData = randomizedDf
      .withColumn("join_key", (col("id") + idOffset).cast(LongType))
      .withColumn("random_string_1", expr("uuid()")) // Efficiently generate a random UUID string
      .withColumn("random_string_2", substring(sha2(concat(col("id").cast("string"),
        lit("salt")), 256), 0, 10))
      .withColumn("random_int_1", (rand() * 100000).cast("int"))
      .withColumn("random_int_2", (rand() * 5000).cast("int"))
      .withColumn("random_long_1", (rand() * 10000000000L).cast(LongType))
      .withColumn("random_double_1", rand() * 100.0)
      .withColumn("random_double_2", randn()) // Random number from a standard normal distribution
      .withColumn("timestamp", current_timestamp())
      .drop("id") // Drop the original id column as join_key is our new identifier

    // 3. Write the DataFrame to disk in Parquet format.
    //    - Using overwrite mode for repeatable runs.
    //    - Parquet is the default format, but it's good practice to be explicit.
    println(s"Writing ${dfWithData.count()} rows for $tableName to $outputPath...")
    dfWithData.write
      .mode("overwrite")
      .parquet(outputPath)

    println(s"--- Finished writing $tableName ---")
  }

  def main(args: Array[String]): Unit = {
    // Initialize Spark Session
    val spark = SparkSession.builder()
      .appName("Scala Large Join Benchmark")
      .getOrCreate()

    // Import spark implicits for handy functions like $.

    // --- Configuration ---
    // Let's estimate the number of rows needed for ~100GB.
    // A row with this schema is roughly 100 bytes.
    // 100GB / 100 bytes/row = 1 billion rows.
    val numRowsTableA: Long = 1000000000L
    val numRowsTableB: Long = 1000000000L

    // Set the base path in your distributed file system (e.g., HDFS, S3, ADLS)
    val basePath = "s3a://karuppayyar-sf/spark-benchmark-data" // CHANGE THIS

    val tableA = "table_a"
    val tableB = "table_b"
    // --- Data Generation ---

    spark.time {
      createAndWriteTable(spark, tableA, numRowsTableA, basePath)
    }
    // For table B, we'll offset the keys so that about half the keys overlap with table_a
    spark.time {
      createAndWriteTable(spark, tableB, numRowsTableB, basePath,
        idOffset = numRowsTableA / 2)
    }


    (1 to 2).foreach { i =>
      Seq( true, false).foreach(enabled => {
        println(s"Iteration: $i")

        spark.time {
          spark.sql(s"set ${SQLConf.SHUFFLE_PARTITIONS.key}=200")
          spark.sql(s"set ${SQLConf.ENABLE_SHUFFLE_CONSOLIDATION.key}=$enabled")

          // --- Join Operation ---
          println("\n--- Starting Join Operation ---")

          // 1. Read the two tables back from Parquet
          println("Reading table_a...")
          val tableA_DF = spark.read.parquet(s"$basePath/$tableA")

          println("Reading table_b...")
          val tableB_DF = spark.read.parquet(s"$basePath/$tableB")

          // 2. Perform the join
          //    This is a wide transformation and will trigger a significant shuffle.
          println("Performing the join on 'join_key'...")
          val joinedDF = tableA_DF.join(tableB_DF, Seq("join_key"), "inner")

          // 3. Trigger an action to execute the join and get the result count.
          //    The .count() action will force Spark to execute the entire DAG.
          println("Noop write of the results of the join...")
          joinedDF.write.format("noop").mode(SaveMode.Overwrite).save()
        }
      })
    }
  }
}
