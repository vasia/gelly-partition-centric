/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.graph.vertex.centric;

import org.apache.flink.api.common.accumulators.Histogram;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.partition.centric.PCGraph;
import org.apache.flink.types.NullValue;

/**
 * This is an implementation of the Single-Source-Shortest Paths algorithm, using a vertex-centric iteration.
 */
@SuppressWarnings("serial")
public class SingleSourceShortestPaths<K> implements GraphAlgorithm<K, Double, Double, DataSet<Vertex<K, Double>>> {
    public static final String MESSAGE_SENT_CTR = "long:message_sent";
    public static final String MESSAGE_SENT_ITER_CTR = "histogram:message_sent_iter_ctr";
    public static final String ACTIVE_VER_ITER_CTR = "histogram:active_ver_iter_ctr";

    private final K srcVertexId;
    private final Integer maxIterations;

    /**
     * Creates an instance of the SingleSourceShortestPaths algorithm.
     *
     * @param srcVertexId The ID of the source vertex.
     * @param maxIterations The maximum number of iterations to run.
     */
    public SingleSourceShortestPaths(K srcVertexId, Integer maxIterations) {
        this.srcVertexId = srcVertexId;
        this.maxIterations = maxIterations;
    }

    @Override
    public DataSet<Vertex<K, Double>> run(Graph<K, Double, Double> input) {

        Graph<K, Double, Double> g = input.mapVertices(new InitVerticesMapper<K>(srcVertexId));
        PCGraph<K, Double, Double> pcGraph = new PCGraph<>(g);
        return pcGraph.runVertexCentricIteration(
                new VertexDistanceUpdater<K>(),
                new MinDistanceMessenger<K>(),
                    maxIterations).getVertices();
    }

    public static final class InitVerticesMapper<K>	implements MapFunction<Vertex<K, Double>, Double> {

        private K srcVertexId;

        public InitVerticesMapper(K srcId) {
            this.srcVertexId = srcId;
        }

        public Double map(Vertex<K, Double> value) {
            if (value.f0.equals(srcVertexId)) {
                return 0.0;
            } else {
                return Double.MAX_VALUE;
            }
        }
    }

    /**
     * Function that updates the value of a vertex by picking the minimum
     * distance from all incoming messages.
     *
     * @param <K>
     */
    public static final class VertexDistanceUpdater<K> extends VertexUpdateFunction<K, Double, Double> {

        @Override
        public void updateVertex(Vertex<K, Double> vertex,
                                 MessageIterator<Double> inMessages) {

            Double minDistance = Double.MAX_VALUE;

            for (double msg : inMessages) {
                if (msg < minDistance) {
                    minDistance = msg;
                }
            }

            if (vertex.getValue() > minDistance) {
                setNewVertexValue(minDistance);
            }
        }
    }

    /**
     * Distributes the minimum distance associated with a given vertex among all
     * the target vertices summed up with the edge's value.
     *
     * @param <K>
     */
    public static final class MinDistanceMessenger<K> extends MessagingFunction<K, Double, Double, Double> {

        @Override
        public void preSuperstep() throws Exception {
            runtimeContext.addAccumulator(MESSAGE_SENT_CTR, new LongCounter());
            runtimeContext.addAccumulator(MESSAGE_SENT_ITER_CTR, new Histogram());
            runtimeContext.addAccumulator(ACTIVE_VER_ITER_CTR, new Histogram());
        }

        @Override
        public void sendMessages(Vertex<K, Double> vertex)
                throws Exception {

            // send current minimum to neighbors
            Histogram messageHistogram = runtimeContext.getHistogram(MESSAGE_SENT_ITER_CTR);
            LongCounter messageCounter = runtimeContext.getLongCounter(MESSAGE_SENT_CTR);
            Histogram vertexHistogram = runtimeContext.getHistogram(ACTIVE_VER_ITER_CTR);

            if (vertexHistogram != null) {
                vertexHistogram.add(getSuperstepNumber());
            }

            for (Edge<K, Double> edge : getEdges()) {
                if (messageCounter != null) {
                    messageCounter.add(1);
                }
                if (messageHistogram != null) {
                    messageHistogram.add(getSuperstepNumber());
                }
                sendMessageTo(edge.getTarget(), vertex.getValue() + edge.getValue());
            }
        }
    }
}