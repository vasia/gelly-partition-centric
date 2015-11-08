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

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.partition.centric.PCGraph;
import org.apache.flink.graph.partition.centric.PCVertex;
import org.apache.flink.graph.partition.centric.PartitionMessagingFunction;
import org.apache.flink.graph.partition.centric.PartitionUpdateFunction;

import java.util.*;

/**
 * Example for partition centric iteration
 */
public class PartitionCentricConnectedComponent {

    public static void main(String[] args) throws Exception {
        Collection<Vertex<Integer, Integer>> vertices = Arrays.asList(
                new Vertex<>(1, 1),
                new Vertex<>(2, 2),
                new Vertex<>(3, 3),
                new Vertex<>(4, 4),
                new Vertex<>(5, 5),
                new Vertex<>(6, 6),
                new Vertex<>(7, 7),
                new Vertex<>(8, 8),
                new Vertex<>(9, 9)
        );
        Collection<Edge<Integer, Integer>> edges = Arrays.asList(
                new Edge<>(1, 2, 1),
                new Edge<>(2, 1, 1),
                new Edge<>(2, 3, 1),
                new Edge<>(3, 2, 1),
                new Edge<>(3, 1, 1),
                new Edge<>(1, 3, 1),
                new Edge<>(3, 4, 1),
                new Edge<>(4, 3, 1),
                new Edge<>(5, 6, 1),
                new Edge<>(6, 5, 1),
                new Edge<>(6, 7, 1),
                new Edge<>(7, 6, 1)
        );

        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();

        Graph<Integer, Integer, Integer> graph = Graph.fromCollection(vertices, edges, environment);

        PCGraph<Integer, Integer, Integer> pcGraph = PCGraph.fromGraph(graph);

        PCGraph<Integer, Integer, Integer> result =
                pcGraph.runPartitionCentricIteration(new CCPartitionUpdateFunction(), new CCPartitionMessagingFunction(), 10);

        List<PCVertex<Integer, Integer, Integer>> ret = result.getVertices().collect();

        for(PCVertex<Integer, Integer, Integer> vertex: ret) {
            System.out.printf("Vertex id: %d, value: %d%n",
                    vertex.getId(), vertex.getValue());
        }
    }

    /**
     * Partition update function
     */
    public static class CCPartitionUpdateFunction extends PartitionUpdateFunction<Integer, Integer, Integer, Integer> {
        private static final long serialVersionUID = 1L;

        @Override
        public void updatePartition(
                Iterable<PCVertex<Integer, Integer, Integer>> vertices,
                Iterable<Tuple3<Long, Integer, Integer>> inMessages) throws Exception {
            Map<Integer, Integer> messageMap = new HashMap<>();
            for(Tuple3<Long, Integer, Integer> message: inMessages) {
                messageMap.put(message.f1, message.f2);
            }

            boolean updatedVertex = false;

            for(PCVertex<Integer, Integer, Integer> vertex: vertices) {
                // We have a message incoming
                if (messageMap.containsKey(vertex.getId())) {
                    int messageValue = messageMap.get(vertex.getId());
                    if (messageValue < vertex.getValue()) {
                        vertex.setValue(messageValue);
                        updatedVertex = true;
                    }
                }
            }

            // Run connected component on the partition
            PriorityQueue<PCVertex<Integer, Integer, Integer>> pq = new PriorityQueue<>(new Comparator<PCVertex<Integer, Integer, Integer>>() {
                @Override
                public int compare(PCVertex<Integer, Integer, Integer> o1, PCVertex<Integer, Integer, Integer> o2) {
                    return o1.getValue().compareTo(o2.getValue());
                }
            });

            // Update priority queue to min value
            Map<Integer, PCVertex<Integer, Integer, Integer>> verticesMap = new HashMap<>();
            for(PCVertex<Integer, Integer, Integer> vertex: vertices) {
                pq.add(vertex);
                verticesMap.put(vertex.getId(), vertex);
            }
            while (!pq.isEmpty()) {
                PCVertex<Integer, Integer, Integer> top = pq.poll();
                for(Map.Entry<Integer, Integer> edge: top.getEdges().entrySet()) {
                    if (verticesMap.containsKey(edge.getKey())) {
                        PCVertex<Integer, Integer, Integer> item = verticesMap.get(edge.getKey());
                        if (item.getValue() > top.getValue()) {
                            pq.remove(item);
                            item.setValue(top.getValue());
                            pq.add(item);
                            updatedVertex = true;
                        }
                    }
                }
            }

            if (updatedVertex) {
                updatePartition(verticesMap.values());
            }
        }
    }

    /**
     * Partition messaging function
     */
    public static class CCPartitionMessagingFunction extends PartitionMessagingFunction<Integer, Integer, Integer, Integer> {
        private static final long serialVersionUID = 1L;

        @Override
        public void sendMessages() {
            for(PCVertex<Integer, Integer, Integer> sourceVertex: sourcePartition) {
                HashMap<Integer, Integer> outGoing = sourceVertex.getEdges();
                for(Map.Entry<Integer, Integer> edge: outGoing.entrySet()) {
                    // Only send message when the the vertex is in a different partition
                    if (!isSamePartition(edge.getKey())) {
                        sendMessageTo(edge.getKey(), sourceVertex.getValue());
                    }
                }
            }
        }
    }
}
