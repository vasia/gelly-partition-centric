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

package org.apache.flink.graph.partition.centric.example;

import java.util.Arrays;
import java.util.Collection;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.partition.centric.PCConnectedComponents;

/**
 * Example for partition centric iteration
 */
public class PartitionCentricConnectedComponent {

    public static void main(String[] args) throws Exception {
        Collection<Vertex<Integer, Long>> vertices = Arrays.asList(
                new Vertex<>(1, 1L),
                new Vertex<>(2, 2L),
                new Vertex<>(3, 3L),
                new Vertex<>(4, 4L),

                new Vertex<>(5, 5L),
                new Vertex<>(6, 6L),
                new Vertex<>(7, 7L),
                new Vertex<>(10, 10L),

                new Vertex<>(8, 8L),
                new Vertex<>(9, 9L)
        );
        Collection<Edge<Integer, Integer>> edges = Arrays.asList(
                new Edge<>(1, 2, 1),
                new Edge<>(2, 3, 1),
                new Edge<>(3, 1, 1),
                new Edge<>(3, 4, 1),
                new Edge<>(5, 6, 1),
                new Edge<>(7, 6, 1)
        );

        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();

        environment.getConfig().disableSysoutLogging();

        Graph<Integer, Long, Integer> graph = Graph.fromCollection(vertices, edges, environment);

        PCConnectedComponents<Integer, Integer> connectedComponents = new PCConnectedComponents<>(10);
        connectedComponents.run(graph).print();

    }
}
