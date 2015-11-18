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

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.library.ConnectedComponents;
import org.apache.flink.graph.partition.centric.PCConnectedComponents;
import org.apache.flink.graph.partition.centric.PCVertex;
import org.apache.flink.graph.partition.centric.utils.GraphGenerator;
import org.apache.flink.test.testdata.ConnectedComponentsData;
import org.apache.flink.types.NullValue;

import java.util.List;

/**
 * Compare performance between vertex centric and partition centric implementation
 * of ConnectedComponents algorithm
 */
public class ConnectedComponentsCompare {

    public static void main(String[] args) throws Exception {
        int verticesCount = 40000;
        int edgesCount = verticesCount * 20;

        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
//        environment.getConfig().disableSysoutLogging();

        Graph<Long, Long, Long> graph = GraphGenerator.generateGraph(verticesCount, edgesCount, environment);

        long start1 = System.currentTimeMillis();
        PCConnectedComponents<Long, Long> algo = new PCConnectedComponents<>(verticesCount);
        List<Tuple2<Long, Long>> result = algo.run(graph).map(
                new RichMapFunction<Vertex<Long, Long>, Tuple2<Long, Long>>() {
                    @Override
                    public Tuple2<Long, Long> map(Vertex<Long, Long> value) throws Exception {
                        return new Tuple2<>(value.getId(), value.getValue());
                    }
                }).collect();
        ConnectedComponentsData.checkOddEvenResult(result);
        System.out.printf("Partition centric: %d ms%n", System.currentTimeMillis() - start1);

        long start2 = System.currentTimeMillis();
        ConnectedComponents<Long, Long> vcAlgo = new ConnectedComponents<>(verticesCount);
        List<Tuple2<Long, Long>> result2 = vcAlgo.run(graph).map(
                new RichMapFunction<Vertex<Long, Long>, Tuple2<Long, Long>>() {
                    @Override
                    public Tuple2<Long, Long> map(Vertex<Long, Long> value) throws Exception {
                        return new Tuple2<>(value.getId(), value.getValue());
                    }
                }).collect();
        ConnectedComponentsData.checkOddEvenResult(result2);
        System.out.printf("Vertex centric: %d ms%n", System.currentTimeMillis() - start2);
    }
}
