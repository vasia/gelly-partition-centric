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

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Connected components algorithm implemented using partition centric iteration
 *
 * @param <K>
 * @param <EV>
 */
public class PCConnectedComponents<K, EV> implements
        GraphAlgorithm<K, Long, EV, DataSet<PCVertex<K, Long, EV>>> {

    private int maxIteration;

    public PCConnectedComponents(int maxIteration) {
        this.maxIteration = maxIteration;
    }

    @Override
    public DataSet<PCVertex<K, Long, EV>> run(Graph<K, Long, EV> input) throws Exception {
        PCGraph<K, Long, EV> pcGraph = PCGraph.fromGraph(input);

        PCGraph<K, Long, EV> result =
                pcGraph.runPartitionCentricIteration(
                        new CCPartitionUpdateFunction<K, EV>(),
                        new CCPartitionMessagingFunction<K, EV>(),
                        maxIteration);

        return result.getVertices();
    }

    /**
     * Partition update function
     */
    public static final class CCPartitionUpdateFunction<K, EV> extends
            PartitionUpdateFunction<K, Long, Long, EV> {
        private static final long serialVersionUID = 1L;
        private static final Logger LOG = LoggerFactory.getLogger(CCPartitionUpdateFunction.class);

        @Override
        public void updateVertex(Iterable<Tuple2<PCVertex<K, Long, EV>, ArrayList<Long>>> v) throws Exception {
            ArrayList<PCVertex<K, Long, EV>> vertices = new ArrayList<>();
            Set<PCVertex<K, Long, EV>> updated = new HashSet<>();
            for (Tuple2<PCVertex<K, Long, EV>, ArrayList<Long>> pair : v) {
                PCVertex<K, Long, EV> vertex = pair.f0;
                for (Long message : pair.f1) {
                    if (vertex.getValue() > message) {
                        vertex.setValue(message);
                        updated.add(vertex);
                    }
                }
                vertices.add(vertex);
            }

            // Run connected component on the partition
            PriorityQueue<PCVertex<K, Long, EV>> pq = new PriorityQueue<>(
                    new Comparator<PCVertex<K, Long, EV>>() {
                        @Override
                        public int compare(PCVertex<K, Long, EV> o1, PCVertex<K, Long, EV> o2) {
                            return o1.getValue().compareTo(o2.getValue());
                        }
                    });

            // Update priority queue to min value
            Map<K, PCVertex<K, Long, EV>> verticesMap = new HashMap<>();
            for (PCVertex<K, Long, EV> vertex : vertices) {
                pq.add(vertex);
                verticesMap.put(vertex.getId(), vertex);
            }
            while (!pq.isEmpty()) {
                PCVertex<K, Long, EV> top = pq.poll();
                for (Map.Entry<K, EV> edge : top.getEdges().entrySet()) {
                    if (verticesMap.containsKey(edge.getKey())) {
                        PCVertex<K, Long, EV> item = verticesMap.get(edge.getKey());
                        if (item.getValue() > top.getValue()) {
                            pq.remove(item);
                            item.setValue(top.getValue());
                            updated.add(item);
                            pq.add(item);
                        }
                    }
                }
            }

            // Mark the vertex as updated
            for (PCVertex<K, Long, EV> vertex : updated) {
                updateVertex(vertex);
            }
        }
    }

    /**
     * Partition messaging function
     */
    public static class CCPartitionMessagingFunction<K, EV> extends PartitionMessagingFunction<K, Long, Long, EV> {
        private static final long serialVersionUID = 1L;
        private static final Logger LOG = LoggerFactory.getLogger(CCPartitionUpdateFunction.class);

        @Override
        public void sendMessages() {
            // Run connected component on the partition
            for (Map.Entry<K, EV> edge : sourceVertex.getEdges().entrySet()) {
                // External vertices, send message to update
                sendMessageTo(edge.getKey(), sourceVertex.getValue());
            }
        }
    }
}
