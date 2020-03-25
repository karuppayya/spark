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

import java.io.{BufferedInputStream, IOException}
import java.nio.charset.Charset

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.input.FileSplit


private[redshift] class RedshiftRecordReader extends RecordReader[Long, Array[String]] {

  @inline private[this] final val escapeChar: Byte = '\\'
  @inline private[this] final val lineFeed: Byte = '\n'
  @inline private[this] final val carriageReturn: Byte = '\r'
  @inline private[this] final val defaultBufferSize = 1024 * 1024
  private[this] val chars = ArrayBuffer.empty[Byte]
  private var reader: BufferedInputStream = _
  private var key: Long = _
  private var value: Array[String] = _
  private var start: Long = _
  private var end: Long = _
  private var cur: Long = _
  private var eof: Boolean = false
  private var delimiter: Byte = _

  override def initialize(
      inputSplit: InputSplit, context: TaskAttemptContext): Unit = {
    val split = inputSplit.asInstanceOf[FileSplit]
    val file = split.getPath
    val conf: Configuration = context.getConfiguration
    delimiter = Utils.getDelimiter(conf).asInstanceOf[Byte]
    require(delimiter != escapeChar,
      s"The delimiter and the escape char cannot be the same but found $delimiter.")
    require(delimiter != lineFeed, "The delimiter cannot be the lineFeed character.")
    require(delimiter != carriageReturn, "The delimiter cannot be the carriage return.")
    val compressionCodecs = new CompressionCodecFactory(conf)
    val codec = compressionCodecs.getCodec(file)
    if (codec != null) {
      throw new IOException(s"Do not support compressed files but found $file.")
    }
    val fs = file.getFileSystem(conf)
    val size = fs.getFileStatus(file).getLen
    // FIXME
    start = findNext(fs, file, size, 0L)
    end = findNext(fs, file, size, size)
    cur = start
    val in = fs.open(file)
    if (cur > 0L) {
      in.seek(cur - 1L)
      in.read()
    }
    reader = new BufferedInputStream(in, defaultBufferSize)
  }

  /**
   * Finds the start of the next record.
   * Because we don't know whether the first char is escaped or not, we need to first find a
   * position that is not escaped.
   *
   * @param fs     file system
   * @param file   file path
   * @param size   file size
   * @param offset start offset
   * @return the start position of the next record
   */
  private def findNext(fs: FileSystem, file: Path, size: Long, offset: Long): Long = {
    if (offset == 0L) {
      return 0L
    } else if (offset >= size) {
      return size
    }
    val in = fs.open(file)
    var pos = offset
    in.seek(pos)
    val bis = new BufferedInputStream(in, defaultBufferSize)
    // Find the first unescaped char.
    var escaped = true
    var thisEof = false
    while (escaped && !thisEof) {
      val v = bis.read()
      if (v < 0) {
        thisEof = true
      } else {
        pos += 1
        if (v != escapeChar) {
          escaped = false
        }
      }
    }
    // Find the next unescaped line feed.
    var endOfRecord = false
    while ((escaped || !endOfRecord) && !thisEof) {
      val v = bis.read()
      if (v < 0) {
        thisEof = true
      } else {
        pos += 1
        if (v == escapeChar) {
          escaped = true
        } else {
          if (!escaped) {
            endOfRecord = v == lineFeed
          } else {
            escaped = false
          }
        }
      }
    }
    in.close()
    pos
  }

  override def getProgress: Float = {
    if (start >= end) {
      1.0f
    } else {
      math.min((cur - start).toFloat / (end - start), 1.0f)
    }
  }

  override def nextKeyValue(): Boolean = {
    if (cur < end && !eof) {
      key = cur
      value = nextValue()
      true
    } else {
      key = 0L
      value = null
      false
    }
  }

  private def nextValue(): Array[String] = {
    val fields = ArrayBuffer.empty[String]
    var escaped = false
    var endOfRecord = false
    while (!endOfRecord && !eof) {
      var endOfField = false
      chars.clear()
      while (!endOfField && !endOfRecord && !eof) {
        val v = reader.read()
        if (v < 0) {
          eof = true
        } else {
          cur += 1L
          val c = v.asInstanceOf[Byte]
          if (escaped) {
            if (c != escapeChar && c != delimiter && c != lineFeed && c != carriageReturn) {
              throw new IllegalStateException(
                s"Found `$c` (ASCII $v) after $escapeChar.")
            }
            chars.append(c)
            escaped = false
          } else {
            if (c == escapeChar) {
              escaped = true
            } else if (c == delimiter) {
              endOfField = true
            } else if (c == lineFeed) {
              endOfRecord = true
            } else {
              // also copy carriage return
              chars.append(c)
            }
          }
        }
      }
      // TODO: charset?
      fields.append(new String(chars.toArray, Charset.forName("UTF-8")))
    }
    if (escaped) {
      throw new IllegalStateException(s"Found hanging escape char.")
    }
    fields.toArray
  }

  override def getCurrentValue: Array[String] = value

  override def getCurrentKey: Long = key

  override def close(): Unit = {
    if (reader != null) {
      reader.close()
    }
  }
}
