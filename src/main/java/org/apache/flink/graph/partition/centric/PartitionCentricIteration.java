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

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichCoGroupFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.CustomUnaryOperation;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        CustomUnaryOperation<PCVertex<K, VV, EV>, PCVertex<K, VV, EV>> {

    private static final Logger LOG = LoggerFactory.getLogger(PartitionCentricIteration.class);

    private final PartitionUpdateFunction<K, VV, Message, EV> updateFunction;

    private final PartitionMessagingFunction<K, VV, Message, EV> messagingFunction;

    private final VertexUpdateFunction<K, VV, Message, EV> vertexUpdateFunction;

    private final int maxIteration;

    private final TypeInformation<Message> messageType;

    private DataSet<PCVertex<K, VV, EV>> initialVertices;

    public PartitionCentricIteration(
            PartitionUpdateFunction<K, VV, Message, EV> updateFunction,
            PartitionMessagingFunction<K, VV, Message, EV> messagingFunction,
            VertexUpdateFunction<K, VV, Message, EV> vertexUpdateFunction,
            int maxIteration) {
        this.updateFunction = updateFunction;
        this.messagingFunction = messagingFunction;
        this.vertexUpdateFunction = vertexUpdateFunction;
        this.maxIteration = maxIteration;
        this.messageType = getMessageType(messagingFunction);
    }

    @Override
    public void setInput(DataSet<PCVertex<K, VV, EV>> inputData) {
        this.initialVertices = inputData;
    }

    @Override
    public DataSet<PCVertex<K, VV, EV>> createResult() {
        if (this.initialVertices == null) {
            throw new RuntimeException("Initial vertices not set");
        }
        TypeInformation<PCVertex<K, VV, EV>> vertexType = initialVertices.getType();
        TypeInformation<K> keyType = ((TupleTypeInfo<?>) vertexType).getTypeAt(0);

        // Start the iteration
        DeltaIteration<PCVertex<K, VV, EV>, PCVertex<K, VV, EV>> iteration =
                initialVertices.iterateDelta(initialVertices, maxIteration, 0);
        iteration.name("Partition-centric iteration (" + updateFunction + " | " + messagingFunction + ")");

        // Update the partition
        PartitionUpdateUdf<K, VV, EV, Message> partitionUpdater =
                new PartitionUpdateUdf<>(updateFunction, initialVertices.getType());
        DataSet<PCVertex<K, VV, EV>> updatedVertex =
                iteration.getWorkset().mapPartition(partitionUpdater);

        // Build the messages to pass to each vertex
        TypeInformation<Tuple2<K, Message>> messageTypeInfo = new TupleTypeInfo<>(keyType, messageType);
        MessagingUdf<K, VV, Message, EV> messenger = new MessagingUdf<>(messagingFunction, messageTypeInfo);
        FlatMapOperator<?, Tuple2<K, Message>> messages =
                updatedVertex.flatMap(messenger);

        // Combine the messages and deliver the result to each vertex
        DataSet<PCVertex<K, VV, EV>> newSolution = messages.coGroup(iteration.getSolutionSet())
                        .where(0).equalTo(0)
                        .with(new VertexUpdateUdf<>(vertexUpdateFunction));

        DataSet<PCVertex<K, VV, EV>> newWorkset = newSolution.filter(new FilterFunction<PCVertex<K, VV, EV>>() {
            @Override
            public boolean filter(PCVertex<K, VV, EV> value) throws Exception {
                return value.isUpdated();
            }
        });
        // Finish iteration
        return iteration.closeWith(newSolution, newWorkset);
    }

    private TypeInformation<Message> getMessageType(PartitionMessagingFunction<K, VV, Message, EV> mf) {
        return TypeExtractor.createTypeInfo(PartitionMessagingFunction.class, mf.getClass(), 2, null, null);
    }

    /**
     * Wrap the user-defined messaging function
     *
     * @param <K>
     * @param <VV>
     * @param <Message>
     * @param <EV>
     */
    private static class MessagingUdf<K, VV, Message, EV> extends
            RichFlatMapFunction<PCVertex<K, VV, EV>, Tuple2<K, Message>> implements
            ResultTypeQueryable<Tuple2<K, Message>> {
        private static final long serialVersionUID = 1L;

        final PartitionMessagingFunction<K, VV, Message, EV> messagingFunction;

        private transient TypeInformation<Tuple2<K, Message>> messageType;

        public MessagingUdf(PartitionMessagingFunction<K, VV, Message, EV> messagingFunction,
                            TypeInformation<Tuple2<K, Message>> messageTypeInfo) {
            this.messagingFunction = messagingFunction;
            this.messageType = messageTypeInfo;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            int currentStep = getIterationRuntimeContext().getSuperstepNumber();
            if (currentStep == 1) {
                this.messagingFunction.init();
            }
            this.messagingFunction.setCurrentStep(currentStep);
        }

        @Override
        public TypeInformation<Tuple2<K, Message>> getProducedType() {
            return this.messageType;
        }

        @Override
        public void flatMap(PCVertex<K, VV, EV> value, Collector<Tuple2<K, Message>> out) throws Exception {
            messagingFunction.setCollector(out);
            messagingFunction.setSourceVertex(value);
            messagingFunction.sendMessages();
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
            PCVertex<K, VV, EV>,
            PCVertex<K, VV, EV>> implements
            ResultTypeQueryable<PCVertex<K, VV, EV>> {
        private static final long serialVersionUID = 1L;

        private final PartitionUpdateFunction<K, VV, Message, EV> updateFunction;
        private transient TypeInformation<PCVertex<K, VV, EV>> resultType;

        private PartitionUpdateUdf(PartitionUpdateFunction<K, VV, Message, EV> updateFunction, TypeInformation<PCVertex<K, VV, EV>> resultType) {
            this.updateFunction = updateFunction;
            this.resultType = resultType;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            this.updateFunction.setCurrentStep(getIterationRuntimeContext().getSuperstepNumber());
            LOG.debug("Starting update iteration {}", getIterationRuntimeContext().getSuperstepNumber());
        }

        @Override
        public TypeInformation<PCVertex<K, VV, EV>> getProducedType() {
            return resultType;
        }

        @Override
        public void mapPartition(Iterable<PCVertex<K, VV, EV>> values, Collector<PCVertex<K, VV, EV>> out) throws Exception {
            updateFunction.setCollector(out);
            updateFunction.updateVertex(values);
        }
    }

    private static class VertexUpdateUdf<K, Message, VV, EV> extends
            RichCoGroupFunction<
                    Tuple2<K, Message>, PCVertex<K, VV, EV>,
                    PCVertex<K, VV, EV>> {
        private final VertexUpdateFunction<K, VV, Message, EV> vertexUpdateFunction;

        private VertexUpdateUdf(VertexUpdateFunction<K, VV, Message, EV> vertexUpdateFunction) {
            this.vertexUpdateFunction = vertexUpdateFunction;
        }

        @Override
        public void coGroup(Iterable<Tuple2<K, Message>> first, Iterable<PCVertex<K, VV, EV>> second, Collector<PCVertex<K, VV, EV>> out) throws Exception {
            Iterator<PCVertex<K, VV, EV>> vertexIterator = second.iterator();
            if (vertexIterator.hasNext()) {
                vertexUpdateFunction.setCollector(out);
                vertexUpdateFunction.setVertex(vertexIterator.next());
                vertexUpdateFunction.updateVertex(first);
            } else {
                throw new RuntimeException("Invalid vertex");
            }
        }
    }
}
