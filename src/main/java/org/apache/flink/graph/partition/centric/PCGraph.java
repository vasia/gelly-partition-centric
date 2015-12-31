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
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;

/**
 * Represent a partition centric graph.
 *
 * @param <K>  The type of a vertex's id
 * @param <VV> The type of a vertex's value
 * @param <EV> The type of an edge's value
 */
public class PCGraph<K, VV, EV> {

    private final Graph<K, VV, EV> graph;

    public PCGraph(Graph<K, VV, EV> graph) {
        this.graph = graph;
    }

    public<Message> Graph<K, VV, EV> runPartitionCentricIteration(
            PartitionProcessFunction<K, VV, Message, EV> updateFunction,
            VertexUpdateFunction<K, VV, Message, EV> vertexUpdateFunction,
            int maximumNumOperations) {
        DataSet<Edge<K, EV>> edges = graph.getEdges();
        DataSet<Vertex<K, VV>> vertices = graph.getVertices();

        PartitionCentricIteration<K, VV, Message, EV> iteration = new PartitionCentricIteration<>(
                updateFunction, vertexUpdateFunction, maximumNumOperations, edges);

        DataSet<Vertex<K, VV>> updatedVertices = vertices.runOperation(iteration);

        return Graph.fromDataSet(updatedVertices, edges, graph.getContext());
    }
}
