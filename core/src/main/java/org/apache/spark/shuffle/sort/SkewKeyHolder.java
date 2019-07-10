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

public class SkewKeyHolder {

    private int partitionId;
    private Object currentValue = null;
    // say all records are unique, then
    // currentCount > count will never be true
    private long currentCount = 1;

    // Key will never be update when
    // 1. No elements in theis partition
    // 2. When there is only one element in the partition
    // corresponding to this skew holder
    private Object key;
    private long count = -1;

    public SkewKeyHolder(int partitionId) {
        this.partitionId = partitionId;
    }

    /**
     * Very Important: Dont add any compute intensive task here
     * Dont touch this method unless it is absolutely necessary
     * Keep code minimal
     * @param value
     */
    public void update(Object value) {
        // currentValue != value , expensive?
        if (currentValue != value) {
            if (currentCount > count) {
                count = currentCount;
                key = currentValue;
            }
            currentValue = value;
            currentCount = 0;
        }
        currentCount ++;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public Object getKey() {
        Object retObj = key;
        if (key == null) {
            retObj = currentValue;
        }
        return retObj;
    }

    public long getCount() {
        long retCount = count;
        if (key == null) {
            retCount = currentCount;
        }
        return retCount;
    }

}
