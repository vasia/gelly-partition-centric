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

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.RichCoGroupFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsFirst;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsSecond;
import org.apache.flink.api.java.operators.CustomUnaryOperation;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;

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

    private final PartitionProcessFunction<K, VV, Message, EV> partitionProcessFunction;

    private final VertexUpdateFunction<K, VV, Message, EV> vertexUpdateFunction;

    private final int maxIteration;

    private final TypeInformation<Message> messageType;

    private DataSet<Vertex<K, VV>> initialVertices;
    private final DataSet<Edge<K, EV>> edges;

    public PartitionCentricIteration(
            PartitionProcessFunction<K, VV, Message, EV> partitionProcessFunction,
            VertexUpdateFunction<K, VV, Message, EV> vertexUpdateFunction,
            int maxIteration, DataSet<Edge<K, EV>> edges) {
        this.partitionProcessFunction = partitionProcessFunction;
        this.vertexUpdateFunction = vertexUpdateFunction;
        this.maxIteration = maxIteration;
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
        TypeInformation<K> keyType = ((TupleTypeInfo<?>) vertexType).getTypeAt(0);
        TypeInformation<Tuple2<K, Message>> messageTypeInfo = new TupleTypeInfo<>(keyType, messageType);

        // Start the iteration
        DeltaIteration<Vertex<K, VV>, Vertex<K, VV>> iteration =
                initialVertices.iterateDelta(initialVertices, maxIteration, 0);
        String defaultName = "Partition-centric iteration (" + partitionProcessFunction + " | " + vertexUpdateFunction + ")";
        iteration.name(defaultName);

        // Prepare the partition input
        DataSet<RichEdge<K, VV, EV>> vertexEdges = iteration.getWorkset()
        	.coGroup(edges).where(0).equalTo(0).with(new PreparePartitionInput<K, VV, EV>());

        // Update the partition, receive a dataset of message
        PartitionUpdateUdf<K, VV, EV, Message> partitionUpdater =
        		new PartitionUpdateUdf<>(partitionProcessFunction, messageTypeInfo);

        DataSet<Tuple2<K, Message>> messages = vertexEdges.mapPartition(partitionUpdater);

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
            RichEdge<K, VV, EV>, Tuple2<K, Message>> implements ResultTypeQueryable<Tuple2<K, Message>> {

        private static final long serialVersionUID = 1L;

        private final PartitionProcessFunction<K, VV, Message, EV> updateFunction;
        private transient TypeInformation<Tuple2<K, Message>> resultType;

        private PartitionUpdateUdf(PartitionProcessFunction<K, VV, Message, EV> updateFunction,
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
        public void mapPartition(Iterable<RichEdge<K, VV, EV>> values, Collector<Tuple2<K, Message>> out) throws Exception {
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
    @SuppressWarnings("serial")
	@ForwardedFieldsSecond("f0")
    private static class VertexUpdateUdf<K, Message, VV, EV> extends
            RichCoGroupFunction<Tuple2<K, Message>, Vertex<K, VV>, Vertex<K, VV>> {

        final VertexUpdateFunction<K, VV, Message, EV> vertexUpdateFunction;
		final MessageIterator<Message> messageIter = new MessageIterator<Message>();

        private VertexUpdateUdf(VertexUpdateFunction<K, VV, Message, EV> vertexUpdateFunction) {
            this.vertexUpdateFunction = vertexUpdateFunction;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            if (getIterationRuntimeContext().getSuperstepNumber() == 1) {
                this.vertexUpdateFunction.init(getIterationRuntimeContext());
            }
            this.vertexUpdateFunction.preSuperstep();
        }

        @Override
        public void close() throws Exception {
            this.vertexUpdateFunction.postSuperstep();
        }

        @Override
        public void coGroup(Iterable<Tuple2<K, Message>> messages, Iterable<Vertex<K, VV>> vertex,
        		Collector<Vertex<K, VV>> out) throws Exception {

			final Iterator<Vertex<K, VV>> vertexIter = vertex.iterator();

			if (vertexIter.hasNext()) {
				Vertex<K, VV> vertexState = vertexIter.next();

				@SuppressWarnings("unchecked")
				Iterator<Tuple2<?, Message>> downcastIter = (Iterator<Tuple2<?, Message>>) (Iterator<?>) messages.iterator();
				messageIter.setSource(downcastIter);

				vertexUpdateFunction.setOutput(vertexState, out);
				vertexUpdateFunction.updateVertex(vertexState, messageIter);
			}
			else {
				final Iterator<Tuple2<K, Message>> messageIter = messages.iterator();
				if (messageIter.hasNext()) {
					String message = "Target vertex does not exist!.";
					try {
						Tuple2<K, Message> next = messageIter.next();
						message = "Target vertex '" + next.f0 + "' does not exist!.";
					} catch (Throwable t) {}
					throw new Exception(message);
				} else {
					throw new Exception();
				}
			}
		}
    }

    
	@SuppressWarnings("serial")
	@ForwardedFieldsFirst("f0->f0; f1->f1")
    @ForwardedFieldsSecond("f2->f2; f1->f3")
	private static class PreparePartitionInput<K, VV, EV> implements
    		CoGroupFunction<Vertex<K, VV>, Edge<K, EV>, RichEdge<K, VV, EV>> {

		public void coGroup(Iterable<Vertex<K, VV>> vertex,
			Iterable<Edge<K, EV>> edges, Collector<RichEdge<K, VV, EV>> out) {
			
			final Iterator<Vertex<K, VV>> vertexIt = vertex.iterator();
			if (vertexIt.hasNext()) {
				Vertex<K, VV> v = vertexIt.next();
				for (Edge<K, EV> e: edges) {
					out.collect(new RichEdge<K, VV, EV>(
						v.getId(), v.getValue(), e.getValue(), e.getTarget()));	
				}
			}
		}
	}
}
