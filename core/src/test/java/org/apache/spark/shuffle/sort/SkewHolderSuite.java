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

package org.apache.spark.shuffle.sort;

import org.apache.spark.SparkConf;
import org.apache.spark.internal.config.package$;
import org.apache.spark.memory.*;
import org.apache.spark.unsafe.memory.MemoryBlock;
import org.junit.Test;

import java.io.IOException;

import static org.apache.spark.shuffle.sort.PackedRecordPointer.MAXIMUM_PAGE_SIZE_BYTES;
import static org.apache.spark.shuffle.sort.PackedRecordPointer.MAXIMUM_PARTITION_ID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class SkewHolderSuite {

  @Test
  public void offsetsPastMaxOffsetInPageWillOverflow() {
    SkewKeyHolder holder = new SkewKeyHolder(0, null);
    Integer[] values = new Integer[] {
            new Integer(0),
            new Integer(1),
            new Integer(1),
            new Integer(1),
    };
    for (int i = 0; i< values.length; i++) {
      holder.update(values[i]);
    }
    assertEquals(holder.getCount(), 3);
    assertEquals(holder.getKey(), new Integer(1));
  }
}
