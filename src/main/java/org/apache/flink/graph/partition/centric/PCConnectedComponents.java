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

import org.apache.flink.api.common.accumulators.Histogram;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.utils.NullValueEdgeMapper;
import org.apache.flink.types.NullValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Connected components algorithm implemented using partition centric iteration
 *
 * @param <K>
 * @param <EV>
 */
public class PCConnectedComponents<K, EV> implements
        GraphAlgorithm<K, Long, EV, DataSet<Vertex<K, Long>>> {

    public static final String MESSAGE_SENT_CTR = "long:message_sent";
    public static final String MESSAGE_SENT_ITER_CTR = "histogram:message_sent_iter_ctr";
    public static final String ACTIVE_VER_ITER_CTR = "histogram:active_ver_iter_ctr";
    public static final String ITER_CTR = "long:iteration_counter";

    private int maxIteration;
    private final PartitionCentricConfiguration configuration;

    public PCConnectedComponents(int maxIteration) {
        this.maxIteration = maxIteration;
        this.configuration = new PartitionCentricConfiguration();
    }

    public PCConnectedComponents(int maxIteration, PartitionCentricConfiguration configuration) {
        this.maxIteration = maxIteration;
        this.configuration = configuration;
    }

    @Override
    public DataSet<Vertex<K, Long>> run(Graph<K, Long, EV> input) throws Exception {
        Graph<K, Long, NullValue> undirectedGraph = input.mapEdges(new NullValueEdgeMapper<K, EV>())
                .getUndirected();
        PCGraph<K, Long, NullValue> pcGraph = new PCGraph<>(undirectedGraph);

        Graph<K, Long, NullValue> result =
                pcGraph.runPartitionCentricIteration(
                        new CCPartitionProcessFunction<K, NullValue>(configuration.isTelemetryEnabled()),
                        new CCVertexUpdateFunction<K, NullValue>(),
                        configuration, maxIteration);

        return result.getVertices();
    }

    /**
     * Partition update function
     */
    public static final class CCPartitionProcessFunction<K, EV> extends
            PartitionProcessFunction<K, Long, Long, EV> {
        private static final long serialVersionUID = 1L;
        private static final Logger LOG = LoggerFactory.getLogger(CCPartitionProcessFunction.class);
        private final boolean telemetryEnabled;

        public CCPartitionProcessFunction() {
            telemetryEnabled = false;
        }

        public CCPartitionProcessFunction(boolean telemetryEnabled) {
            this.telemetryEnabled = telemetryEnabled;
        }

        @Override
        public void preSuperstep() {
            if (telemetryEnabled) {
                context.addAccumulator(MESSAGE_SENT_CTR, new LongCounter());
                context.addAccumulator(MESSAGE_SENT_ITER_CTR, new Histogram());
                context.addAccumulator(ITER_CTR, new LongCounter());
                context.addAccumulator(ACTIVE_VER_ITER_CTR, new Histogram());
            }
        }

        @Override
        public void processPartition(Iterable<Tuple2<Long, Edge<K, EV>>> vertices) throws Exception {
            HashMap<K, UnionFindNode<Long>> nodeStore = new HashMap<>();
            UnionFind<Long> unionFind = new UnionFind<>();
            for (Tuple2<Long, Edge<K, EV>> i : vertices) {
                Long sourceValue = i.f0;
                Edge<K, EV> edge = i.f1;
                UnionFindNode<Long> node;
                if (nodeStore.containsKey(edge.getSource())) {
                    // This vertex has been inserted as an external node,
                    // update its initial value
                    node = nodeStore.get(edge.getSource());
                    node.initialValue = sourceValue;
                    // Find the root and update its value if needed
                    UnionFindNode<Long> root = unionFind.find(node);
                    if (root.value > sourceValue) {
                        root.value = sourceValue;
                    }
                } else {
                    // New vertex
                    node = unionFind.makeNode(sourceValue);
                    nodeStore.put(edge.getSource(), node);
                }

                // The other end of the edge
                UnionFindNode<Long> otherNode;
                if (nodeStore.containsKey(edge.getTarget())) {
                    // Internal node
                    otherNode = nodeStore.get(edge.getTarget());
                } else {
                    // Probably an external node, insert with the maximum component id
                    otherNode = unionFind.makeNode(Long.MAX_VALUE);
                    nodeStore.put(edge.getTarget(), otherNode);
                }
                // Add the node to the union
                unionFind.union(node, otherNode);
            }

            Histogram messageHistogram = context.getHistogram(MESSAGE_SENT_ITER_CTR);
            LongCounter messageCounter = context.getLongCounter(MESSAGE_SENT_CTR);
            LongCounter iterationCounter = context.getLongCounter(ITER_CTR);
            Histogram vertexHistogram = context.getHistogram(ACTIVE_VER_ITER_CTR);

            if (iterationCounter != null && context.getIndexOfThisSubtask() == 0) {
                iterationCounter.add(1);
            }

            // Send messages to update nodes' value
            for(Map.Entry<K, UnionFindNode<Long>> entry: nodeStore.entrySet()) {
                UnionFindNode<Long> node = entry.getValue();
                K id = entry.getKey();
                Long componentId = unionFind.find(node).value;
                if (!componentId.equals(node.initialValue)) {
                    sendMessage(id, componentId);
                    if (messageCounter != null) {
                        messageCounter.add(1);
                    }
                    if (messageHistogram != null) {
                        messageHistogram.add(context.getSuperstepNumber());
                    }
                }
                // This can give the wrong number, if a vertex appears in multiple parallel partitions
                // So we only count active vertices in the first iteration using this method
                if (vertexHistogram != null &&
                        node.initialValue != Long.MAX_VALUE &&
                        context.getSuperstepNumber() == 1) {
                    vertexHistogram.add(context.getSuperstepNumber());
                }
            }
        }
    }

    public static class CCVertexUpdateFunction<K, EV> extends VertexUpdateFunction<K, Long, Long, EV> {

        @Override
        public void updateVertex(Iterable<Tuple2<K, Long>> message) {
            Histogram vertexHistogram = context.getHistogram(ACTIVE_VER_ITER_CTR);
            Long minValue = vertex.getValue();
            for(Tuple2<K, Long> l: message) {
                if (minValue > l.f1) {
                    minValue = l.f1;
                }
            }
            if (minValue < vertex.getValue()) {
                setVertexValue(minValue);
                // Counting the active vertices in the next iteration
                if (vertexHistogram != null) {
                    vertexHistogram.add(context.getSuperstepNumber() + 1);
                }
            }
        }
    }

    private static class UnionFind<K extends Comparable<K>> {
        public UnionFindNode<K> makeNode(K value) {
            UnionFindNode<K> node = new UnionFindNode<>(value, 0);
            node.parent = node;
            return node;
        }

        public void union(UnionFindNode<K> left, UnionFindNode<K> right) {
            UnionFindNode<K> leftRoot = find(left);
            UnionFindNode<K> rightRoot = find(right);
            if (leftRoot.equals(rightRoot)) {
                return;
            }

            if (leftRoot.rank < rightRoot.rank) {
                leftRoot.parent = rightRoot;
                // The smaller value should be the component id
                if (leftRoot.value.compareTo(rightRoot.value) < 0) {
                    rightRoot.value = leftRoot.value;
                }
            } else if (leftRoot.rank > rightRoot.rank){
                rightRoot.parent = leftRoot;
                // The smaller value should be the component id
                if (leftRoot.value.compareTo(rightRoot.value) > 0) {
                    leftRoot.value = rightRoot.value;
                }
            } else {
                rightRoot.parent = leftRoot;
                leftRoot.rank += 1;
                // The smaller value should be the component id
                if (leftRoot.value.compareTo(rightRoot.value) > 0) {
                    leftRoot.value = rightRoot.value;
                }
            }
        }

        public UnionFindNode<K> find(UnionFindNode<K> n) {
            if (n.parent != n) {
                n.parent = find(n.parent);
            }
            return n.parent;
        }
    }

    private static class UnionFindNode<K extends Comparable<K>> {
        K value;
        K initialValue;
        int rank;
        UnionFindNode<K> parent;

        public UnionFindNode(K value, int rank) {
            this.value = value;
            this.initialValue = value;
            this.rank = rank;
        }
    }
}
