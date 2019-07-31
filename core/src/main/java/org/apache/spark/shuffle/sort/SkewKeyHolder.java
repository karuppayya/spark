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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.math.Ordering;

import java.util.Comparator;

public class SkewKeyHolder<V> {

    private static final Logger logger = LoggerFactory.getLogger(SkewKeyHolder.class);
    private int partitionId;
    private V currentValue = null;
    private long currentCount = 1;

    private Ordering<V> ordering;
    // TODO: if queue size is 1, dont need queue, Optimize!!
    private MinMaxPriorityQueue<SkewInfo> queue;
    private boolean isClosed = false;

    public SkewKeyHolder(int partitionId, Ordering<V> ordering, int queueSize) {
        if (ordering == null) {
            logger.error("Cannot create skew key holder with empty ordering");
            throw new RuntimeException("Specify an ordering");
        }
        this.partitionId = partitionId;
        this.ordering = ordering;
        Comparator<SkewInfo> comparator = new Comparator<SkewInfo>() {
            @Override
            public int compare(SkewInfo o1, SkewInfo o2) {
                long count1 = o1.count();
                long count2 = o2.count();
                return (int)(count2 - count1);
            }
        };
        queue = MinMaxPriorityQueue
                .orderedBy(comparator)
                .maximumSize(queueSize)
                .create();
    }

    public SkewKeyHolder(int partitionId, Ordering<V> ordering) {
        this(partitionId, ordering, 3);
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
        if (ordering.compare(currentValue, value) != 0) {
            queue.add(new SkewInfo(currentValue, currentCount));
            currentValue = value;
            currentCount = 0;
        }
        currentCount++;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public SkewInfo[] getSkewedKeys() {
        if (!isClosed) {
            String msg = "Cannot get skewed keys without closing the holder";
            logger.error(msg);
            throw new RuntimeException(msg);
        }
        assert isClosed : "Holder is not closed";
        SkewInfo[] obj = new SkewInfo[queue.size()];
        return queue.toArray(obj);
    }

    public void close() {
        isClosed = true;
        if (currentValue != null) {
            queue.add(new SkewInfo(currentValue, currentCount));
        }
    }
}
