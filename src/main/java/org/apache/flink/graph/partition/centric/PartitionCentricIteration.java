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

import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.aggregators.Aggregator;
import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.api.common.functions.RichCoGroupFunction;
import org.apache.flink.api.common.functions.RichJoinFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.CustomUnaryOperation;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.operators.JoinOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.partition.centric.utils.IterationTimer;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

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

    public static final String ITER_TIMER = "iteration_timer_acc";
    public static final String ITER_CTR = "long:iteration_counter";

    private static final Logger LOG = LoggerFactory.getLogger(PartitionCentricIteration.class);

    private final PartitionProcessFunction<K, VV, Message, EV> partitionProcessFunction;

    private final VertexUpdateFunction<K, VV, Message, EV> vertexUpdateFunction;

    private final int maxIteration;

    private final TypeInformation<Message> messageType;

    private final PartitionCentricConfiguration configuration;

    private DataSet<Vertex<K, VV>> initialVertices;
    private final DataSet<Edge<K, EV>> edges;

    public PartitionCentricIteration(
            PartitionProcessFunction<K, VV, Message, EV> partitionProcessFunction,
            VertexUpdateFunction<K, VV, Message, EV> vertexUpdateFunction,
            int maxIteration,
            DataSet<Edge<K, EV>> edges, PartitionCentricConfiguration configuration) {
        this.partitionProcessFunction = partitionProcessFunction;
        this.vertexUpdateFunction = vertexUpdateFunction;
        this.maxIteration = maxIteration;
        this.configuration = configuration;
        this.edges = edges;
        this.messageType = getMessageType(partitionProcessFunction);
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
        TypeInformation<Edge<K, EV>> edgeType = edges.getType();
        TypeInformation<K> keyType = ((TupleTypeInfo<?>) vertexType).getTypeAt(0);
        TypeInformation<Tuple2<K, Message>> messageTypeInfo = new TupleTypeInfo<>(keyType, messageType);

        // Start the iteration
        DeltaIteration<Vertex<K, VV>, Vertex<K, VV>> iteration =
                initialVertices.iterateDelta(initialVertices, maxIteration, 0);
        String defaultName = "Partition-centric iteration (" + partitionProcessFunction + " | " + vertexUpdateFunction + ")";
        HashMap<String, Accumulator<?, ?>> accumulators = null;
        boolean telemetryEnabled;
        if (configuration != null) {
            iteration.name(configuration.getName(defaultName));
            // Register aggregator for per-iteration statistic
            for(Map.Entry<String, Aggregator<?>> entry: configuration.getAggregators().entrySet()) {
                iteration.registerAggregator(entry.getKey(), entry.getValue());
            }
            telemetryEnabled = configuration.isTelemetryEnabled();
        } else {
            iteration.name(defaultName);
            telemetryEnabled = false;
        }

        // Join the edges into the vertices
        JoinOperator<?, ?, Tuple2<VV, Edge<K, EV>>> vertexEdges =
                iteration.getWorkset().join(edges).where(0).equalTo(0)
                        .with(
                            new AdjacencyListBuilder<>(
                                new TupleTypeInfo<Tuple2<VV, Edge<K, EV>>>(
                                        ((TupleTypeInfo<?>) vertexType).getTypeAt(1),
                                        edgeType
                                ),
                                telemetryEnabled
                            )
                        );

        // Update the partition, receive a dataset of message
        PartitionUpdateUdf<K, VV, EV, Message> partitionUpdater =
                new PartitionUpdateUdf<>(partitionProcessFunction, messageTypeInfo);
        DataSet<Tuple2<K, Message>> messages =
                vertexEdges.mapPartition(partitionUpdater);

        // Send the message to the vertex for updating, receive a set of updated vertices
        DataSet<Vertex<K, VV>> updatedVertices =
                messages.coGroup(iteration.getSolutionSet())
                        .where(0).equalTo(0)
                        .with(new VertexUpdateUdf<>(vertexUpdateFunction));

        // Finish iteration
        return iteration.closeWith(updatedVertices, updatedVertices);
    }

    private TypeInformation<Message> getMessageType(PartitionProcessFunction<K, VV, Message, EV> puf) {
        return TypeExtractor.createTypeInfo(PartitionProcessFunction.class, puf.getClass(), 2, null, null);
    }

    /**
     * Wrap the user-defined partition update function
     *
     * @param <K>
     * @param <VV>
     * @param <EV>
     * @param <Message>
     */
    private static class PartitionUpdateUdf<K, VV, EV, Message> extends RichMapPartitionFunction<
            Tuple2<VV, Edge<K, EV>>,
            Tuple2<K, Message>> implements
            ResultTypeQueryable<Tuple2<K, Message>> {
        private static final long serialVersionUID = 1L;

        private final PartitionProcessFunction<K, VV, Message, EV> updateFunction;
        private transient TypeInformation<Tuple2<K, Message>> resultType;

        private PartitionUpdateUdf(
                PartitionProcessFunction<K, VV, Message, EV> updateFunction,
                TypeInformation<Tuple2<K, Message>> resultType) {
            this.updateFunction = updateFunction;
            this.resultType = resultType;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            if (getIterationRuntimeContext().getSuperstepNumber() == 1) {
                this.updateFunction.init(getIterationRuntimeContext());
            }
            this.updateFunction.preSuperstep();
        }

        @Override
        public void close() throws Exception {
            this.updateFunction.postSuperStep();
        }

        @Override
        public void mapPartition(Iterable<Tuple2<VV, Edge<K, EV>>> values,
                                 Collector<Tuple2<K, Message>> out) throws Exception {
            updateFunction.setCollector(out);
            updateFunction.processPartition(values);
        }

        @Override
        public TypeInformation<Tuple2<K, Message>> getProducedType() {
            return resultType;
        }
    }

    /**
     * Wrap the user-defined vertex update function
     *
     * @param <K>
     * @param <Message>
     * @param <VV>
     * @param <EV>
     */
    private static class VertexUpdateUdf<K, Message, VV, EV> extends
            RichCoGroupFunction<
                    Tuple2<K, Message>, Vertex<K, VV>,
                    Vertex<K, VV>> {
        private final VertexUpdateFunction<K, VV, Message, EV> vertexUpdateFunction;

        private VertexUpdateUdf(
                VertexUpdateFunction<K, VV, Message, EV> vertexUpdateFunction) {
            this.vertexUpdateFunction = vertexUpdateFunction;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            if (getIterationRuntimeContext().getSuperstepNumber() == 1) {
                this.vertexUpdateFunction.init(getIterationRuntimeContext());
            }
            this.vertexUpdateFunction.preSuperStep();
        }

        @Override
        public void close() throws Exception {
            this.vertexUpdateFunction.postSuperStep();
        }

        @Override
        public void coGroup(Iterable<Tuple2<K, Message>> first, Iterable<Vertex<K, VV>> second, Collector<Vertex<K, VV>> out) throws Exception {
            Iterator<Vertex<K, VV>> vertexIterator = second.iterator();
            if (vertexIterator.hasNext()) {
                Vertex<K, VV> vertex = vertexIterator.next();
                vertexUpdateFunction.setVertex(vertex);
                vertexUpdateFunction.updateVertex(first);
                if (vertexUpdateFunction.isUpdated()) {
                    out.collect(vertex);
                }
            } else {
                throw new RuntimeException("Invalid vertex");
            }
        }
    }

    private static class AdjacencyListBuilder<K, VV, EV> extends
            RichJoinFunction<Vertex<K,VV>, Edge<K, EV>, Tuple2<VV, Edge<K, EV>>> implements
            ResultTypeQueryable<Tuple2<VV, Edge<K, EV>>>{

        private transient final TypeInformation<Tuple2<VV, Edge<K, EV>>> resultType;
        private final boolean telemetryEnabled;

        private AdjacencyListBuilder(TypeInformation<Tuple2<VV, Edge<K, EV>>> resultType, boolean telemetryEnabled) {
            this.resultType = resultType;
            this.telemetryEnabled = telemetryEnabled;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            IterationRuntimeContext context = getIterationRuntimeContext();
            if (telemetryEnabled) {
                context.addAccumulator(ITER_TIMER, new IterationTimer());
                IterationTimer timerAcc = (IterationTimer)
                        context.<Integer, TreeMap<Integer, Long>>getAccumulator(ITER_TIMER);
                timerAcc.add(context.getSuperstepNumber());

                context.addAccumulator(ITER_CTR, new LongCounter());
                LongCounter iterationCounter = context.getLongCounter(ITER_CTR);
                if (iterationCounter != null && context.getIndexOfThisSubtask() == 0) {
                    iterationCounter.add(1);
                }
            }
        }

        @Override
        public Tuple2<VV, Edge<K, EV>> join(Vertex<K, VV> first, Edge<K, EV> second) throws Exception {
            return new Tuple2<>(first.getValue(), second);
        }

        @Override
        public TypeInformation<Tuple2<VV, Edge<K, EV>>> getProducedType() {
            return resultType;
        }
    }

}
