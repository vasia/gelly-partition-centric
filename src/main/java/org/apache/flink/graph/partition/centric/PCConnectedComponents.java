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
import org.apache.flink.graph.utils.NullValueEdgeMapper;
import org.apache.flink.types.NullValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Array;
import java.util.*;

/**
 * Connected components algorithm implemented using partition centric iteration
 *
 * @param <K>
 * @param <EV>
 */
public class PCConnectedComponents<K, EV> implements
        GraphAlgorithm<K, Long, EV, DataSet<PCVertex<K, Long, NullValue>>> {

    private int maxIteration;

    public PCConnectedComponents(int maxIteration) {
        this.maxIteration = maxIteration;
    }

    @Override
    public DataSet<PCVertex<K, Long, NullValue>> run(Graph<K, Long, EV> input) throws Exception {
        Graph<K, Long, NullValue> undirectedGraph = input.mapEdges(new NullValueEdgeMapper<K, EV>())
                .getUndirected();
        PCGraph<K, Long, NullValue> pcGraph = PCGraph.fromGraph(undirectedGraph);

        PCGraph<K, Long, NullValue> result =
                pcGraph.runPartitionCentricIteration(
                        new CCPartitionUpdateFunction<K, NullValue>(),
                        new CCPartitionMessagingFunction<K, NullValue>(),
                        new CCMessageAggregator<K, NullValue>(),
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
        public void updateVertex(Iterable<PCVertex<K, Long, EV>> v) throws Exception {
            HashMap<K, PCVertex<K, Long, EV>> partition = new HashMap<>();
            for (PCVertex<K, Long, EV> vertex : v) {
                partition.put(vertex.getId(), vertex);
            }

            HashMap<K, ArrayList<K>> internalNeighbour = new HashMap<>();
            for (PCVertex<K, Long, EV> vertex: partition.values()) {
                for(Map.Entry<K, EV> edge: vertex.getEdges().entrySet()) {
                    if (partition.containsKey(edge.getKey())) {
                        if (!internalNeighbour.containsKey(vertex.getId())) {
                            internalNeighbour.put(vertex.getId(), new ArrayList<K>());
                        }
                        internalNeighbour.get(vertex.getId()).add(edge.getKey());
                    }
                }
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
            for (PCVertex<K, Long, EV> vertex : partition.values()) {
                pq.add(vertex);
            }
            while (!pq.isEmpty()) {
                PCVertex<K, Long, EV> top = pq.poll();
                if (!internalNeighbour.containsKey(top.getId())) {
                    continue;
                }
                for (K edge : internalNeighbour.get(top.getId())) {
                    PCVertex<K, Long, EV> item = partition.get(edge);
                    if (item.getValue() > top.getValue()) {
                        pq.remove(item);
                        item.setValue(top.getValue());
                        pq.add(item);
                    }
                }
            }

            for (PCVertex<K, Long, EV> vertex : partition.values()) {
                ArrayList<K> externalNeighbour = new ArrayList<>();
                for (Map.Entry<K, EV> edge : vertex.getEdges().entrySet()) {
                    if (!partition.containsKey(edge.getKey())) {
                        externalNeighbour.add(edge.getKey());
                    }
                }
                @SuppressWarnings("unchecked")
                K[] externalNeighbourArray = (K[]) new Object[externalNeighbour.size()];
                externalNeighbour.toArray(externalNeighbourArray);
                updateVertex(vertex, externalNeighbourArray);
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
        public void sendMessages(K[] external) {
            for (K vertex : external) {
                sendMessageTo(vertex, sourceVertex.getValue());
            }
        }
    }

    public static class CCMessageAggregator<K, EV> extends VertexUpdateFunction<K, Long, Long, EV> {

        @Override
        public void updateVertex(Iterable<Tuple2<K, Long>> message) {
            Long minValue = vertex.getValue();
            for(Tuple2<K, Long> l: message) {
                if (minValue > l.f1) {
                    minValue = l.f1;
                }
            }
            if (minValue < vertex.getValue()) {
                setVertexValue(minValue);
            }
        }
    }
}
