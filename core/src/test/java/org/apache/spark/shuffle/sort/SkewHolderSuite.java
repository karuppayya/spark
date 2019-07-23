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

import org.junit.Test;
import scala.math.Ordering;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class SkewHolderSuite {

    @Test
    public void testSkewKeyHolderWithMultipleKeys() {
        SkewKeyHolder<Integer> holder = new SkewKeyHolder(0, Ordering.Int$.MODULE$);
        Integer[] values = new Integer[] {0, 1, 1, 1};
        for (int i = 0; i < values.length; i++) {
            holder.update(values[i]);
        }
        holder.close();
        SkewInfo[] skewedKeys = holder.getSkewedKeys();
        assertEquals(skewedKeys.length, 2);

        SkewInfo[] expectedInfo = new SkewInfo[]{
                new SkewInfo(1, 3),
                new SkewInfo(0, 1)
        };
        assertArrayEquals(skewedKeys, expectedInfo);
    }

    @Test
    public void testSkewKeyHolderWithSingleKey() {
        SkewKeyHolder<Integer> holder = new SkewKeyHolder(0, Ordering.Int$.MODULE$);
        Integer[] values = new Integer[] {1, 1, 1, 1};

        for (int i = 0; i < values.length; i++) {
            holder.update(values[i]);
        }
        holder.close();
        SkewInfo[] skewedKeys = holder.getSkewedKeys();
        assertEquals(skewedKeys.length, 1);

        SkewInfo[] expectedInfo = new SkewInfo[]{
                new SkewInfo(1, 4),
        };
        assertArrayEquals(skewedKeys, expectedInfo);
    }

    @Test
    public void testSkewKeyHolderWithNoKey() {
        SkewKeyHolder holder = new SkewKeyHolder(0, Ordering.Int$.MODULE$);
        holder.close();
        SkewInfo[] skewedKeys = holder.getSkewedKeys();
        assertEquals(skewedKeys.length, 0);
    }

    @Test(expected = RuntimeException.class)
    public void cannotGetSkewedKeysWithoutClosingHolder() {
        SkewKeyHolder holder = new SkewKeyHolder(0, Ordering.Int$.MODULE$);
        holder.getSkewedKeys();
    }

    @Test(expected = RuntimeException.class)
    public void cannotCreateSkewHolderWithNullOrdering() {
        new SkewKeyHolder<Integer>(0, null);
    }
}
