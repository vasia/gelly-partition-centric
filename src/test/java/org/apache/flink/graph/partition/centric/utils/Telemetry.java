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
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.library.ConnectedComponents;
import org.apache.flink.graph.partition.centric.PCConnectedComponents;
import org.apache.flink.graph.partition.centric.PartitionCentricIteration;
import org.apache.flink.test.testdata.ConnectedComponentsData;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Utilities class for Telemetry purpose
 */
public class Telemetry {
    public static void printTelemetry(String name, JobExecutionResult result, Map<String, String> fields) {
        System.out.println(name);
        System.out.printf("Execution time: %s ms%n", result.getNetRuntime());
        for(Map.Entry<String, String> field: fields.entrySet()) {
            if (field.getKey().equals(PartitionCentricIteration.ITER_TIMER)) {
                TreeMap<Integer, Long> out = result.getAccumulatorResult(field.getKey());
                for (int i = 1; i <= out.size(); i++) {
                    if (out.containsKey(i+1)) {
                        System.out.printf("%s iteration %d: %d ms%n",
                                field.getValue(), i, out.get(i+1) - out.get(i));
                    } else {
                        System.out.printf("%s iteration %d: %d ms%n",
                                field.getValue(), i, System.currentTimeMillis() - out.get(i));
                    }
                }
            } else if (field.getKey().startsWith("long")) {
                Long count = result.getAccumulatorResult(field.getKey());
                System.out.printf("%s: %d%n",
                        field.getValue(),
                        count);
            } else if (field.getKey().startsWith("histogram")) {
                TreeMap<Integer, Integer> out = result.getAccumulatorResult(field.getKey());
                for(Map.Entry<Integer, Integer> item: out.entrySet()) {
                    System.out.printf("%s iteration %d: %d%n",
                            field.getValue(), item.getKey(), item.getValue());
                }
            }
        }
    }

    public static boolean dummyPCConnectedComponent(ExecutionEnvironment environment) throws Exception {
        int verticesCount = 40000;
        int edgesCount = verticesCount * 10;
        Graph<Long, Long, Long> graph = GraphGenerator.generateGraph(verticesCount, edgesCount, environment);
        PCConnectedComponents<Long, Long> algo = new PCConnectedComponents<>(verticesCount);
        long result = algo.run(graph).count();
        return result == verticesCount;
    }

    public static boolean dummyVCConnectedComponent(ExecutionEnvironment environment) throws Exception {
        int verticesCount = 40000;
        int edgesCount = verticesCount * 10;
        Graph<Long, Long, Long> graph = GraphGenerator.generateGraph(verticesCount, edgesCount, environment);
        ConnectedComponents<Long, Long> vcAlgo = new ConnectedComponents<>(verticesCount);
        long result = vcAlgo.run(graph).count();
        return result == verticesCount;
    }
}
