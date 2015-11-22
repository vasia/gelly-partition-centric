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

package org.apache.flink.graph.partition.centric.performance;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.library.ConnectedComponents;
import org.apache.flink.graph.partition.centric.PCConnectedComponents;
import org.apache.flink.types.NullValue;

/**
 * Testing the PCConnectedComponents on Twitter Munmun dataset
 */
public class WebGoogle {
    public static void main(String[] args) throws Exception {

        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
//        environment.getConfig().disableSysoutLogging();

        Graph<Long, Long, NullValue> graph = Graph
                .fromCsvReader(
                        "data/web-Google/web-Google.data",
                        new MapFunction<Object, Object>() {
                            @Override
                            public Object map(Object value) throws Exception {
                                return value;
                            }
                        }, environment)
                .fieldDelimiterEdges("\t")
                .lineDelimiterEdges("\n")
                .ignoreCommentsEdges("#")
                .vertexTypes(Long.class, Long.class);

        long start = System.currentTimeMillis();
        PCConnectedComponents<Long, NullValue> algo = new PCConnectedComponents<>(Integer.MAX_VALUE);
        algo.run(graph).writeAsCsv("out/pcwebgoogle", FileSystem.WriteMode.OVERWRITE);
        environment.execute();
        System.out.printf("Elapsed time: %d ms%n", System.currentTimeMillis() - start);

        start = System.currentTimeMillis();
        ConnectedComponents<Long, NullValue> vcAlgo = new ConnectedComponents<>(Integer.MAX_VALUE);
        vcAlgo.run(graph).writeAsCsv("out/vcwebgoogle", FileSystem.WriteMode.OVERWRITE);
        environment.execute();
        System.out.printf("Elapsed time: %d ms%n", System.currentTimeMillis() - start);
    }
}
