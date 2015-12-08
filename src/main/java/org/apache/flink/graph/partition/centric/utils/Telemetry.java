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

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.vertex.centric.ConnectedComponents;
import org.apache.flink.graph.partition.centric.PCConnectedComponents;
import org.apache.flink.graph.partition.centric.PartitionCentricIteration;

import java.util.*;

/**
 * Utilities class for Telemetry purpose
 */
public class Telemetry {
    public static void printTelemetry(String name, List<JobExecutionResult> results, Map<String, String> fields) {
        System.out.println(name);
        long runtime = 0;
        Map<String, Map<Integer, Long>> summary = new HashMap<>();
        for(JobExecutionResult result: results) {
            runtime += result.getNetRuntime();
            for(Map.Entry<String, String> field: fields.entrySet()) {
                if (!summary.containsKey(field.getKey())) {
                    summary.put(field.getKey(), new HashMap<Integer, Long>());
                }
                Map<Integer, Long> summaryByKey = summary.get(field.getKey());
                if (field.getKey().equals(PartitionCentricIteration.ITER_TIMER)) {
                    TreeMap<Integer, Long> out = result.getAccumulatorResult(field.getKey());
                    for (int i = 1; i <= out.size(); i++) {
                        long timeTotal = 0;
                        if (out.containsKey(i+1)) {
                            if (summaryByKey.containsKey(i)) {
                                 timeTotal += summaryByKey.get(i);
                            }
                            timeTotal += out.get(i+1) - out.get(i);
                        }
                        summaryByKey.put(i, timeTotal);
                    }
                } else if (field.getKey().startsWith("long")) {
                    long total = 0;
                    if (summaryByKey.containsKey(0)) {
                        total += summaryByKey.get(0);
                    }
                    long tmp = result.getAccumulatorResult(field.getKey());
                    total += tmp;
                    summaryByKey.put(0, total);
                } else if (field.getKey().startsWith("histogram")) {
                    TreeMap<Integer, Integer> out = result.getAccumulatorResult(field.getKey());
                    for(Map.Entry<Integer, Integer> item: out.entrySet()) {
                        long total = 0;
                        if (summaryByKey.containsKey(item.getKey())) {
                            total += summaryByKey.get(item.getKey());
                        }
                        total += item.getValue();
                        summaryByKey.put(item.getKey(), total);
                    }
                }
            }
        }

        System.out.printf("Execution time: %d ms%n", runtime / results.size());
        for (Map.Entry<String, String> field: fields.entrySet()) {
            if (!summary.containsKey(field.getKey())) {
                continue;
            }
            Map<Integer, Long> summaryByKey = summary.get(field.getKey());
            if (field.getKey().equals(PartitionCentricIteration.ITER_TIMER)) {
                for(int i = 1; i <= summaryByKey.size(); i++) {
                    System.out.printf("%s iteration %d: %d ms%n",
                            field.getValue(),
                            i,
                            summaryByKey.get(i) / results.size());
                }
            } else if (field.getKey().startsWith("long")) {
                System.out.printf("%s: %d%n",
                        field.getValue(),
                        summaryByKey.get(0) / results.size());
            } else if (field.getKey().startsWith("histogram")) {
                for (int i = 1; i <= summaryByKey.size(); i++) {

                    System.out.printf("%s iteration %d: %d%n",
                            field.getValue(), i, summaryByKey.get(i) / results.size());
                }
            }
        }
    }
}
