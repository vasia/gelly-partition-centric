/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.graph.partition.centric.utils;

import org.apache.flink.api.common.accumulators.Accumulator;

import java.util.Map;
import java.util.TreeMap;

/**
 * Accumulator class to time the duration of each iteration
 */
public class IterationTimer implements Accumulator<Integer, TreeMap<Integer, Long>> {

    private TreeMap<Integer, Long> treeMap = new TreeMap<>();

    @Override
    public void add(Integer value) {
        long currentTime = System.currentTimeMillis();
        treeMap.put(value, currentTime);
    }

    @Override
    public TreeMap<Integer, Long> getLocalValue() {
        return treeMap;
    }

    @Override
    public void resetLocal() {
        treeMap.clear();
    }

    @Override
    public void merge(Accumulator<Integer, TreeMap<Integer, Long>> other) {
        for (Map.Entry<Integer, Long> entryFromOther : other.getLocalValue().entrySet()) {
            Long ownValue = this.treeMap.get(entryFromOther.getKey());
            if (ownValue == null) {
                this.treeMap.put(entryFromOther.getKey(), entryFromOther.getValue());
            } else {
                this.treeMap.put(entryFromOther.getKey(), (entryFromOther.getValue() + ownValue) / 2);
            }
        }
    }

    @Override
    public Accumulator<Integer, TreeMap<Integer, Long>> clone() {
        IterationTimer out = new IterationTimer();
        out.treeMap = new TreeMap<>(treeMap);
        return out;
    }

}
