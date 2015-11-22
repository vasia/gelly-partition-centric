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
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.utils.NullValueEdgeMapper;
import org.apache.flink.types.NullValue;
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
        GraphAlgorithm<K, Long, EV, DataSet<Vertex<K, Long>>> {

    private int maxIteration;

    public PCConnectedComponents(int maxIteration) {
        this.maxIteration = maxIteration;
    }

    @Override
    public DataSet<Vertex<K, Long>> run(Graph<K, Long, EV> input) throws Exception {
        Graph<K, Long, NullValue> undirectedGraph = input.mapEdges(new NullValueEdgeMapper<K, EV>())
                .getUndirected();
        PCGraph<K, Long, NullValue> pcGraph = new PCGraph<>(undirectedGraph);

        Graph<K, Long, NullValue> result =
                pcGraph.runPartitionCentricIteration(
                        new CCPartitionProcessFunction<K, NullValue>(),
                        new CCVertexUpdateFunction<K, NullValue>(),
                        maxIteration);

        return result.getVertices();
    }

    /**
     * Partition update function
     */
    public static final class CCPartitionProcessFunction<K, EV> extends
            PartitionProcessFunction<K, Long, Long, EV> {
        private static final long serialVersionUID = 1L;
        private static final Logger LOG = LoggerFactory.getLogger(CCPartitionProcessFunction.class);

        @Override
        public void processPartition(Iterable<Tuple2<Vertex<K, Long>, HashMap<K, EV>>> vertices) throws Exception {
            HashMap<K, UnionFindNode<Long>> nodeStore = new HashMap<>();
            UnionFind<Long> unionFind = new UnionFind<>();
            for (Tuple2<Vertex<K, Long>, HashMap<K, EV>> i : vertices) {
                Vertex<K, Long> vertex = i.f0;
                UnionFindNode<Long> node;
                if (nodeStore.containsKey(vertex.getId())) {
                    // This vertex has been inserted as an external node,
                    // update its initial value
                    node = nodeStore.get(vertex.getId());
                    node.initialValue = vertex.getValue();
                    // Find the root and update its value if needed
                    UnionFindNode<Long> root = unionFind.find(node);
                    if (root.value > vertex.getValue()) {
                        root.value = vertex.getValue();
                    }
                } else {
                    // New vertex
                    node = unionFind.makeNode(vertex.getValue());
                    nodeStore.put(vertex.getId(), node);
                }

                for(Map.Entry<K, EV> edge: i.f1.entrySet()) {
                    // The other end of the edge
                    UnionFindNode<Long> otherNode;
                    if (nodeStore.containsKey(edge.getKey())) {
                        // Internal node
                        otherNode = nodeStore.get(edge.getKey());
                    } else {
                        // Probably an external node, insert with the maximum component id
                        otherNode = unionFind.makeNode(Long.MAX_VALUE);
                        nodeStore.put(edge.getKey(), otherNode);
                    }
                    // Add the node to the union
                    unionFind.union(node, otherNode);
                }
            }

            // Send messages to update nodes' value
            for(Map.Entry<K, UnionFindNode<Long>> entry: nodeStore.entrySet()) {
                UnionFindNode<Long> node = entry.getValue();
                K id = entry.getKey();
                Long componentId = unionFind.find(node).value;
                if (!componentId.equals(node.initialValue)) {
                    sendMessage(id, componentId);
                }
            }
        }
    }

    public static class CCVertexUpdateFunction<K, EV> extends VertexUpdateFunction<K, Long, Long, EV> {

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
