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

package org.apache.flink.graph.partition.centric;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.test.testdata.ConnectedComponentsData;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Scanner;
import java.util.Set;

/**
 * Automated test for connected components algorithm
 *
 */
public class PCConnectedComponentsTest {

    @Test
    public void testRun() throws Exception {
        int verticesCount = 5000;
        int edgesCount = verticesCount * 2;

        String vertices = ConnectedComponentsData.getEnumeratingVertices(verticesCount);
        Scanner scanner = new Scanner(vertices);
        Set<Vertex<Long, Long>> verticesSet = new HashSet<>();
        while(scanner.hasNext()) {
            Vertex<Long, Long> vertex = new Vertex<>();
            long vertexId = scanner.nextLong();
            vertex.setId(vertexId);
            vertex.setValue(vertexId);
            verticesSet.add(vertex);
        }

        String edges = ConnectedComponentsData.getRandomOddEvenEdges(edgesCount, verticesCount, 0);
        scanner = new Scanner(edges);
        Set<Edge<Long, Long>> edgesSet = new HashSet<>();
        while(scanner.hasNext()) {
            long source = scanner.nextLong();
            long target = scanner.nextLong();

            Edge<Long, Long> edge = new Edge<>(source, target, 1L);
            Edge<Long, Long> reverseEdge = new Edge<>(target, source, 1L);
            edgesSet.add(edge);
            edgesSet.add(reverseEdge);
        }

        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        environment.getConfig().disableSysoutLogging();

        Graph<Long, Long, Long> graph = Graph.fromCollection(verticesSet, edgesSet, environment);

        PCConnectedComponents<Long, Long> algo = new PCConnectedComponents<>(verticesCount);

        List<Tuple2<Long, Long>> result = algo.run(graph).map(
                new RichMapFunction<PCVertex<Long, Long, Long>, Tuple2<Long, Long>>() {
                    @Override
                    public Tuple2<Long, Long> map(PCVertex<Long, Long, Long> value) throws Exception {
                        return new Tuple2<>(value.getId(), value.getValue());
                    }
        }).collect();

        ConnectedComponentsData.checkOddEvenResult(result);
    }
}