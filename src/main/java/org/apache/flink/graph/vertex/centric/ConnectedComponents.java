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

package org.apache.flink.graph.vertex.centric;

import org.apache.flink.api.common.accumulators.Histogram;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.*;
import org.apache.flink.graph.partition.centric.PCGraph;
import org.apache.flink.graph.utils.NullValueEdgeMapper;
import org.apache.flink.types.NullValue;

/**
 * A vertex-centric implementation of the Weakly Connected Components algorithm.
 *
 * This implementation assumes that the vertex values of the input Graph are initialized with Long component IDs.
 * The vertices propagate their current component ID in iterations.
 * Upon receiving component IDs from its neighbors, a vertex adopts a new component ID if its value
 * is lower than its current component ID.
 *
 * The algorithm converges when vertices no longer update their component ID value
 * or when the maximum number of iterations has been reached.
 *
 * The result is a DataSet of vertices, where the vertex value corresponds to the assigned component ID.
 *
 * @see org.apache.flink.graph.library.GSAConnectedComponents
 */
@SuppressWarnings("serial")
public class ConnectedComponents<K, EV> implements GraphAlgorithm<K, Long, EV, DataSet<Vertex<K, Long>>> {
    public static final String MESSAGE_SENT_CTR = "long:message_sent";
    public static final String MESSAGE_SENT_ITER_CTR = "histogram:message_sent_iter_ctr";
    public static final String ACTIVE_VER_ITER_CTR = "histogram:active_ver_iter_ctr";

    private Integer maxIterations;

    /**
     * Creates an instance of the Connected Components algorithm.
     * The algorithm computes weakly connected components
     * and converges when no vertex updates its component ID
     * or when the maximum number of iterations has been reached.
     *
     * @param maxIterations The maximum number of iterations to run.
     */
    public ConnectedComponents(Integer maxIterations) {
        this.maxIterations = maxIterations;
    }

    @Override
    public DataSet<Vertex<K, Long>> run(Graph<K, Long, EV> graph) throws Exception {


        Graph<K, Long, NullValue> undirectedGraph = graph.mapEdges(new NullValueEdgeMapper<K, EV>())
                .getUndirected();
        PCGraph<K, Long, NullValue> pcGraph = new PCGraph<>(undirectedGraph);

        // initialize vertex values and run the Vertex Centric Iteration
        return pcGraph.runVertexCentricIteration(
                new CCUpdater<K>(), new CCMessenger<K>(), maxIterations)
                .getVertices();
    }

    /**
     * Updates the value of a vertex by picking the minimum neighbor ID out of all the incoming messages.
     */
    public static final class CCUpdater<K> extends VertexUpdateFunction<K, Long, Long> {

        @Override
        public void updateVertex(Vertex<K, Long> vertex, MessageIterator<Long> messages) throws Exception {
            long min = Long.MAX_VALUE;

            for (long msg : messages) {
                min = Math.min(min, msg);
            }

            // update vertex value, if new minimum
            if (min < vertex.getValue()) {
                setNewVertexValue(min);
            }
        }
    }

    /**
     * Distributes the minimum ID associated with a given vertex among all the target vertices.
     */
    public static final class CCMessenger<K> extends MessagingFunction<K, Long, Long, NullValue> {

        @Override
        public void preSuperstep() throws Exception {
            runtimeContext.addAccumulator(MESSAGE_SENT_CTR, new LongCounter());
            runtimeContext.addAccumulator(MESSAGE_SENT_ITER_CTR, new Histogram());
            runtimeContext.addAccumulator(ACTIVE_VER_ITER_CTR, new Histogram());
        }

        @Override
        public void sendMessages(Vertex<K, Long> vertex) throws Exception {
            // send current minimum to neighbors
            Histogram messageHistogram = runtimeContext.getHistogram(MESSAGE_SENT_ITER_CTR);
            LongCounter messageCounter = runtimeContext.getLongCounter(MESSAGE_SENT_CTR);
            Histogram vertexHistogram = runtimeContext.getHistogram(ACTIVE_VER_ITER_CTR);

            if (vertexHistogram != null) {
                vertexHistogram.add(getSuperstepNumber());
            }

            Long m = vertex.getValue();
            Iterable<Edge<K, NullValue>> edges = getEdges();
            for(Edge<K, NullValue> next: edges) {
                if (messageCounter != null) {
                    messageCounter.add(1);
                }
                if (messageHistogram != null) {
                    messageHistogram.add(getSuperstepNumber());
                }
                sendMessageTo(next.getTarget(), m);
            }
        }
    }
}