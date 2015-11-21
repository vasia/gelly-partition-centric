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

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.CoGroupOperator;
import org.apache.flink.api.java.operators.CustomUnaryOperation;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;

/**
 * Implementation of partition centric iteration
 *
 * @param <K>
 * @param <VV>
 * @param <Message>
 * @param <EV>
 */
public class PartitionCentricIteration<K, VV, Message, EV> implements
        CustomUnaryOperation<Vertex<K, VV>, Vertex<K, VV>> {

    private static final Logger LOG = LoggerFactory.getLogger(PartitionCentricIteration.class);

    private final PartitionUpdateFunction<K, VV, Message, EV> updateFunction;

    private final VertexUpdateFunction<K, VV, Message, EV> vertexUpdateFunction;

    private final int maxIteration;

    private final TypeInformation<Message> messageType;

    private DataSet<Vertex<K, VV>> initialVertices;
    private final DataSet<Edge<K, EV>> edges;

    public PartitionCentricIteration(
            PartitionUpdateFunction<K, VV, Message, EV> updateFunction,
            VertexUpdateFunction<K, VV, Message, EV> vertexUpdateFunction,
            int maxIteration, DataSet<Edge<K, EV>> edges) {
        this.updateFunction = updateFunction;
        this.vertexUpdateFunction = vertexUpdateFunction;
        this.maxIteration = maxIteration;
        this.edges = edges;
        this.messageType = getMessageType(updateFunction);
    }

    @Override
    public void setInput(DataSet<Vertex<K, VV>> inputData) {
        this.initialVertices = inputData;
    }

    @Override
    public DataSet<Vertex<K, VV>> createResult() {
        if (this.initialVertices == null) {
            throw new RuntimeException("Initial vertices not set");
        }
        TypeInformation<Vertex<K, VV>> vertexType = initialVertices.getType();
        TypeInformation<K> keyType = ((TupleTypeInfo<?>) vertexType).getTypeAt(0);
        TypeInformation<Tuple2<K, Message>> messageTypeInfo = new TupleTypeInfo<>(keyType, messageType);

        @SuppressWarnings("unchecked")
        TypeInformation<PartitionUpdateOutputBean<K, VV, Message>> puType =
                (TypeInformation<PartitionUpdateOutputBean<K, VV, Message>>) TypeExtractor.createTypeInfo(PartitionUpdateOutputBean.class);

        // Start the iteration
        IterativeDataSet<Vertex<K, VV>> iteration =
                initialVertices.iterate(maxIteration);
        iteration.name("Partition-centric iteration (" + updateFunction + " | " + vertexUpdateFunction + ")");

        // Join the edges into the vertices
        CoGroupOperator<?, ?, Tuple2<Vertex<K, VV>, HashMap<K, EV>>> vertexEdges =
                iteration.coGroup(edges).where(0).equalTo(0).with(new AdjacencyListBuilder<K, VV, EV>());

        // Update the partition
        PartitionUpdateUdf<K, VV, EV, Message> partitionUpdater =
                new PartitionUpdateUdf<>(updateFunction, puType);
        DataSet<PartitionUpdateOutputBean<K, VV, Message>> partitionOutput =
                vertexEdges.mapPartition(partitionUpdater);

        // Rebuild the graph from the partition output result.
        DataSet<Vertex<K, VV>> updatedVertices = partitionOutput.flatMap(
                new GraphRebuilder<K, VV, Message>(initialVertices.getType())
        );

        // Build the messages to pass to each vertex
        FlatMapOperator<?, Tuple2<K, Message>> messages = partitionOutput.flatMap(
                new MessagingUdf<K, VV, Message>(messageTypeInfo)
        );

        // Send the message to the vertex for updating
        DataSet<Tuple2<Vertex<K, VV>, Boolean>> newGraph = messages.coGroup(updatedVertices)
                        .where(0).equalTo(0)
                        .with(new VertexUpdateUdf<>(vertexUpdateFunction));

        // Check if any vertex changed after receiving messages,
        // if not then the iteration can be terminated
        DataSet<Tuple2<Vertex<K, VV>, Boolean>> graphDelta = newGraph.filter(
                new FilterFunction<Tuple2<Vertex<K, VV>, Boolean>>() {
            @Override
            public boolean filter(Tuple2<Vertex<K, VV>, Boolean> value) throws Exception {
                return value.f1;
            }
        });

        updatedVertices = newGraph.map(new MapFunction<Tuple2<Vertex<K, VV>, Boolean>, Vertex<K, VV>>() {
            @Override
            public Vertex<K, VV> map(Tuple2<Vertex<K, VV>, Boolean> value) throws Exception {
                return value.f0;
            }
        });
        // Finish iteration
        return iteration.closeWith(updatedVertices, graphDelta);
    }

    private TypeInformation<Message> getMessageType(PartitionUpdateFunction<K, VV, Message, EV> puf) {
        return TypeExtractor.createTypeInfo(PartitionUpdateFunction.class, puf.getClass(), 2, null, null);
    }

    /**
     * Wrap the user-defined messaging function
     *
     * @param <K>
     * @param <VV>
     * @param <Message>
     */
    private static class MessagingUdf<K, VV, Message> extends
            RichFlatMapFunction<PartitionUpdateOutputBean<K, VV, Message>, Tuple2<K, Message>> implements
            ResultTypeQueryable<Tuple2<K, Message>> {
        private static final long serialVersionUID = 1L;

        private transient TypeInformation<Tuple2<K, Message>> messageType;

        public MessagingUdf(TypeInformation<Tuple2<K, Message>> messageTypeInfo) {
            this.messageType = messageTypeInfo;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
        }

        @Override
        public TypeInformation<Tuple2<K, Message>> getProducedType() {
            return this.messageType;
        }

        @Override
        public void flatMap(PartitionUpdateOutputBean<K, VV, Message> value, Collector<Tuple2<K, Message>> out) throws Exception {
            if (!value.isVertex()) {
                out.collect(value.getMessage());
            }
        }
    }

    /**
     * Wrap the user-defined update function
     *
     * @param <K>
     * @param <VV>
     * @param <EV>
     * @param <Message>
     */
    private static class PartitionUpdateUdf<K, VV, EV, Message> extends RichMapPartitionFunction<
            Tuple2<Vertex<K, VV>, HashMap<K, EV>>,
            PartitionUpdateOutputBean<K, VV, Message>> implements
            ResultTypeQueryable<PartitionUpdateOutputBean<K, VV, Message>> {
        private static final long serialVersionUID = 1L;

        private final PartitionUpdateFunction<K, VV, Message, EV> updateFunction;
        private transient TypeInformation<PartitionUpdateOutputBean<K, VV, Message>> resultType;

        private PartitionUpdateUdf(
                PartitionUpdateFunction<K, VV, Message, EV> updateFunction,
                TypeInformation<PartitionUpdateOutputBean<K, VV, Message>> resultType) {
            this.updateFunction = updateFunction;
            this.resultType = resultType;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            if (getIterationRuntimeContext().getSuperstepNumber() == 1) {
                this.updateFunction.init();
            }
            this.updateFunction.setCurrentStep(getIterationRuntimeContext().getSuperstepNumber());
        }

        @Override
        public void mapPartition(Iterable<Tuple2<Vertex<K, VV>, HashMap<K, EV>>> values,
                                 Collector<PartitionUpdateOutputBean<K, VV, Message>> out) throws Exception {
            updateFunction.setCollector(out);
            updateFunction.updateVertex(values);
        }

        @Override
        public TypeInformation<PartitionUpdateOutputBean<K, VV, Message>> getProducedType() {
            return resultType;
        }
    }

    private static class VertexUpdateUdf<K, Message, VV, EV> extends
            RichCoGroupFunction<
                    Tuple2<K, Message>, Vertex<K, VV>,
                    Tuple2<Vertex<K, VV>, Boolean>> {
        private final VertexUpdateFunction<K, VV, Message, EV> vertexUpdateFunction;

        private VertexUpdateUdf(VertexUpdateFunction<K, VV, Message, EV> vertexUpdateFunction) {
            this.vertexUpdateFunction = vertexUpdateFunction;
        }

        @Override
        public void coGroup(Iterable<Tuple2<K, Message>> first, Iterable<Vertex<K, VV>> second, Collector<Tuple2<Vertex<K, VV>, Boolean>> out) throws Exception {
            Iterator<Vertex<K, VV>> vertexIterator = second.iterator();
            if (vertexIterator.hasNext()) {
                Vertex<K, VV> vertex = vertexIterator.next();
                vertexUpdateFunction.setVertex(vertex);
                vertexUpdateFunction.updateVertex(first);
                out.collect(new Tuple2<>(vertex, vertexUpdateFunction.isUpdated()));
            } else {
                throw new RuntimeException("Invalid vertex");
            }
        }
    }

    private static class AdjacencyListBuilder<K, VV, EV> extends
            RichCoGroupFunction<Vertex<K,VV>, Edge<K,EV>, Tuple2<Vertex<K, VV>, HashMap<K, EV>>> {
        @Override
        public void coGroup(Iterable<Vertex<K, VV>> first,
                            Iterable<Edge<K, EV>> second,
                            Collector<Tuple2<Vertex<K, VV>, HashMap<K, EV>>> out) throws Exception {
            Iterator<Vertex<K, VV>> vertexIterator = first.iterator();
            if (vertexIterator.hasNext()) {
                Vertex<K, VV> vertex = vertexIterator.next();
                HashMap<K, EV> adjacencyList = new HashMap<K, EV>();
                for(Edge<K, EV> e: second) {
                    adjacencyList.put(e.getTarget(), e.getValue());
                }
                out.collect(new Tuple2<>(vertex, adjacencyList));
            }
        }
    }

    private static class GraphRebuilder<K, VV, Message> extends
            RichFlatMapFunction<PartitionUpdateOutputBean<K, VV, Message>, Vertex<K, VV>> implements
            ResultTypeQueryable<Vertex<K, VV>>{
        private final TypeInformation<Vertex<K, VV>> resultType;

        private GraphRebuilder(TypeInformation<Vertex<K, VV>> resultType) {
            this.resultType = resultType;
        }

        @Override
        public void flatMap(PartitionUpdateOutputBean<K, VV, Message> value, Collector<Vertex<K, VV>> out) throws Exception {
            if (value.isVertex()) {
                out.collect(value.getVertex());
            }
        }

        @Override
        public TypeInformation<Vertex<K, VV>> getProducedType() {
            return resultType;
        }
    }
}
