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

import com.google.common.collect.MinMaxPriorityQueue;
import scala.math.Ordering;

import java.util.Comparator;

public class SkewKeyHolder<V> {

    private int partitionId;
    private V currentValue = null;
    // say all records are unique, then
    // currentCount > count will never be true
    private long currentCount = 1;

    // Key will never be update when
    // 1. No elements in theis partition
    // 2. When there is only one element in the partition
    // corresponding to this skew holder
    private V key;
    private long count = -1;
    private Ordering<V> order;
    private MinMaxPriorityQueue<SkewInfo> queue;
    private int size = 3;

    public SkewKeyHolder(int partitionId, Ordering<V> order) {

        this.partitionId = partitionId;
        this.order = order;
        Comparator<SkewInfo> comparator = new Comparator<SkewInfo>() {
            @Override
            public int compare(SkewInfo o1, SkewInfo o2) {
                long count1 = o1.count();
                long count2 = o2.count();
                if (count1 == count2) {
                    return 0;
                } else {
                    return count1 > count2 ? -1: 1;
                }
            }
        };
        queue = MinMaxPriorityQueue
                .orderedBy(comparator)
                .maximumSize(size)
                .create();
    }

    /**
     * Very Important: Dont add any compute intensive task here
     * Dont touch this method unless it is absolutely necessary
     * Keep code minimal
     *
     * @param value the vaue with which skewholder needs to be updated
     */
    public void update(V value) {
        // currentValue != value , expensive?
        if (currentValue == null) {
            currentValue = value;
            currentCount = 0;
        }
        if (order.compare(currentValue, value) != 0) {
            if (currentCount > count) {
                key = currentValue;
                count = currentCount;
                queue.add(new SkewInfo(key, count));
            }
            currentValue = value;
            currentCount = 0;
        }
        currentCount++;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public SkewInfo[] getSkewedKeys() {
        if (key == null ){

           return new SkewInfo[] {
               new SkewInfo(key, count)
           };
        }
        SkewInfo[] obj = new SkewInfo[queue.size()];
        return queue.toArray(obj);
    }
}
