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
import org.apache.flink.graph.Edge;
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
        public void updateVertex(Iterable<Tuple2<Vertex<K, Long>, HashMap<K, EV>>> v) throws Exception {
            HashMap<K, Vertex<K, Long>> partition = new HashMap<>();
            HashMap<K, UnionFindNode<Long>> nodeStore = new HashMap<>();
            UnionFind<Long> unionFind = new UnionFind<>();
            for (Tuple2<Vertex<K, Long>, HashMap<K, EV>> i : v) {
                Vertex<K, Long> vertex = i.f0;
                partition.put(vertex.getId(), vertex);
                nodeStore.put(vertex.getId(), unionFind.makeNode(vertex.getValue()));
                for(Map.Entry<K, EV> edge: i.f1.entrySet()) {
                    if (!nodeStore.containsKey(edge.getKey())) {
                        nodeStore.put(edge.getKey(), unionFind.makeNode(Long.MAX_VALUE));
                    }
                    unionFind.union(nodeStore.get(vertex.getId()), nodeStore.get(edge.getKey()));
                }
            }

            // Group the external nodes according to their value
            Map<Long, ArrayList<K>> outgoing = new HashMap<>();
            for(K id: nodeStore.keySet()) {
                if (!partition.containsKey(id)) {
                    // external node
                    Long value = unionFind.find(nodeStore.get(id)).value;
                    if (!outgoing.containsKey(value)) {
                        outgoing.put(value, new ArrayList<K>());
                    }
                    outgoing.get(value).add(id);
                }
            }

            for (Vertex<K, Long> vertex : partition.values()) {
                UnionFindNode<Long> vNode = nodeStore.get(vertex.getId());
                vertex.setValue(unionFind.find(vNode).value);
                ArrayList<K> externalNeighbour;
                if (outgoing.containsKey(vertex.getValue())) {
                    // This node will take responsibility to send its value to all external nodes
                    // that has the same value as itself
                    externalNeighbour = outgoing.get(vertex.getValue());
                    outgoing.remove(vertex.getValue());
                } else {
                    externalNeighbour = new ArrayList<>();
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

    private static class UnionFind<K extends Comparable<K>> {
        public UnionFindNode<K> makeNode(K value) {
            UnionFindNode<K> node = new UnionFindNode<>(value);
            node.parent = node;
            return node;
        }

        public void union(UnionFindNode<K> left, UnionFindNode<K> right) {
            UnionFindNode<K> leftRoot = find(left);
            UnionFindNode<K> rightRoot = find(right);
            if (leftRoot.equals(rightRoot)) {
                return;
            }
            if (leftRoot.value.compareTo(rightRoot.value) > 0) {
                leftRoot.parent = rightRoot;
            } else {
                rightRoot.parent = leftRoot;
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
        UnionFindNode<K> parent;

        public UnionFindNode(K value) {
            this.value = value;
        }
    }
}
