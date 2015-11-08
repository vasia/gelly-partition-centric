package org.apache.flink.graph.partition.centric;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.CoGroupOperator;
import org.apache.flink.api.java.operators.CustomUnaryOperation;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

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

    private final PartitionUpdateFunction<K, VV, Message, EV> updateFunction;

    private final PartitionMessagingFunction<K, VV, Message, EV> messagingFunction;

    private final int maxIteration;

    private final TypeInformation<Message> messageType;

    private DataSet<PCVertex<K, VV, EV>> initialVertices;

    public PartitionCentricIteration(
            PartitionUpdateFunction<K, VV, Message, EV> updateFunction,
            PartitionMessagingFunction<K, VV, Message, EV> messagingFunction,
            int maxIteration) {
        this.updateFunction = updateFunction;
        this.messagingFunction = messagingFunction;
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
        TypeInformation<K> keyType = ((TupleTypeInfo<?>) initialVertices.getType()).getTypeAt(0);

        // Partition the graph and assign each partition a unique id
        DataSet<Tuple2<Long, HashSet<PCVertex<K, VV, EV>>>> partitions =
                DataSetUtils.zipWithUniqueId(initialVertices.mapPartition(
                        new GraphPartitionFunction<K, VV, EV>())
                );

        // Build a partition map, we need this to detect a vertex is in the same partition or not
        HashMap<K, Long> partitionMap = new HashMap<>();
        try {
            TypeInformation<Tuple2<K, Long>> resultType =
                    new TupleTypeInfo<>(keyType, BasicTypeInfo.LONG_TYPE_INFO);
                        List<Tuple2<K, Long>> tmp =
                    partitions.flatMap(new PartitionMapFunction<K, VV, EV>(resultType)).collect();
            for(Tuple2<K, Long> item: tmp) {
                partitionMap.put(item.f0, item.f1);
            }
        } catch (Exception e) {
            throw new RuntimeException("Unable to build vertex partition mapping", e);
        }

        // Start the iteration
        DeltaIteration<Tuple2<Long, HashSet<PCVertex<K, VV, EV>>>, Tuple2<Long, HashSet<PCVertex<K, VV, EV>>>> iteration =
                partitions.iterateDelta(partitions, maxIteration, 0);
        iteration.name("Partition-centric iteration (" + updateFunction + " | " + messagingFunction + ")");

        // Build the messages to pass to each partitions
        TypeInformation<Tuple3<Long, K, Message>> messageTypeInfo =
                new TupleTypeInfo<>(BasicTypeInfo.LONG_TYPE_INFO, keyType, messageType);
        FlatMapOperator<?, Tuple3<Long, K, Message>> messages =
                iteration.getWorkset().flatMap(new MessagingUdf<>(messagingFunction, partitionMap, messageTypeInfo));

        // Deliver the messages to the partitions
        CoGroupOperator<?, ?, Tuple2<Long, HashSet<PCVertex<K, VV, EV>>>> updates =
                messages.coGroup(iteration.getSolutionSet())
                        .where(0).equalTo(0)
                        .with(new UpdateUdf<>(updateFunction, partitions.getType()));

        // Finish iteration
        DataSet<Tuple2<Long, HashSet<PCVertex<K, VV, EV>>>> result =
                iteration.closeWith(updates, updates);

        // Convert the data set of partitions back to the data set of vertices
        return result.flatMap(new GraphUnpartitionFunction<>(initialVertices.getType()));
    }

    private TypeInformation<Message> getMessageType(PartitionMessagingFunction<K, VV, Message, EV> mf) {
        return TypeExtractor.createTypeInfo(PartitionMessagingFunction.class, mf.getClass(), 2, null, null);
    }

    /**
     * Convert a dataset of partitions to a dataset of vertices
     * 
     * @param <K>
     * @param <VV>
     * @param <EV>
     */
    private static class GraphUnpartitionFunction<K, VV, EV>
            extends RichFlatMapFunction<Tuple2<Long, HashSet<PCVertex<K, VV, EV>>>, PCVertex<K, VV, EV>>
            implements ResultTypeQueryable<PCVertex<K, VV, EV>> {

        private transient TypeInformation<PCVertex<K, VV, EV>> resultType;

        private GraphUnpartitionFunction(TypeInformation<PCVertex<K, VV, EV>> resultType) {
            this.resultType = resultType;
        }

        @Override
        public void flatMap(Tuple2<Long, HashSet<PCVertex<K, VV, EV>>> value, Collector<PCVertex<K, VV, EV>> out) throws Exception {
            for(PCVertex<K, VV, EV> v: value.f1) {
                out.collect(v);
            }
        }

        @Override
        public TypeInformation<PCVertex<K, VV, EV>> getProducedType() {
            return resultType;
        }
    }

    /**
     * Collect a dataset of vertices to a dataset of partitions
     *
     * @param <K>
     * @param <VV>
     * @param <EV>
     */
    private static class GraphPartitionFunction<K, VV, EV> extends
            RichMapPartitionFunction<PCVertex<K, VV, EV>, HashSet<PCVertex<K, VV, EV>>> {

        @Override
        public void mapPartition(
                Iterable<PCVertex<K, VV, EV>> values,
                Collector<HashSet<PCVertex<K, VV, EV>>> out) throws Exception {
            HashSet<PCVertex<K, VV, EV>> partition = new HashSet<>();
            for(PCVertex<K, VV, EV> item: values) {
                partition.add(item);
            }
            out.collect(partition);
        }
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
            RichFlatMapFunction<Tuple2<Long, HashSet<PCVertex<K, VV, EV>>>, Tuple3<Long, K, Message>> implements
            ResultTypeQueryable<Tuple3<Long, K, Message>> {
        private static final long serialVersionUID = 1L;

        final PartitionMessagingFunction<K, VV, Message, EV> messagingFunction;
        final HashMap<K, Long> partitionMap;

        private transient TypeInformation<Tuple3<Long, K, Message>> messageType;

        public MessagingUdf(PartitionMessagingFunction<K, VV, Message, EV> messagingFunction,
                            HashMap<K, Long> partitionMap,
                            TypeInformation<Tuple3<Long, K, Message>> messageTypeInfo) {
            this.messagingFunction = messagingFunction;
            this.partitionMap = partitionMap;
            this.messageType = messageTypeInfo;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            int currentStep = getIterationRuntimeContext().getSuperstepNumber();
            if (currentStep == 1) {
                this.messagingFunction.init(partitionMap);
            }
            this.messagingFunction.setCurrentStep(currentStep);
        }

        @Override
        public TypeInformation<Tuple3<Long, K, Message>> getProducedType() {
            return this.messageType;
        }

        @Override
        public void flatMap(Tuple2<Long, HashSet<PCVertex<K, VV, EV>>> value, Collector<Tuple3<Long, K, Message>> out) throws Exception {
            messagingFunction.setCollector(out);
            messagingFunction.setSourcePartition(value.f1);
            messagingFunction.setPartitionId(value.f0);
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
    private static class UpdateUdf<K, VV, EV, Message> extends RichCoGroupFunction<
            Tuple3<Long, K, Message>,
            Tuple2<Long, HashSet<PCVertex<K, VV, EV>>>,
            Tuple2<Long, HashSet<PCVertex<K, VV, EV>>>> implements
            ResultTypeQueryable<Tuple2<Long, HashSet<PCVertex<K, VV, EV>>>> {
        private static final long serialVersionUID = 1L;

        private final PartitionUpdateFunction<K, VV, Message, EV> updateFunction;
        private transient TypeInformation<Tuple2<Long, HashSet<PCVertex<K, VV, EV>>>> resultType;

        private UpdateUdf(PartitionUpdateFunction<K, VV, Message, EV> updateFunction, TypeInformation<Tuple2<Long, HashSet<PCVertex<K, VV, EV>>>> resultType) {
            this.updateFunction = updateFunction;
            this.resultType = resultType;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            this.updateFunction.setCurrentStep(getIterationRuntimeContext().getSuperstepNumber());
        }

        @Override
        public void coGroup(Iterable<Tuple3<Long, K, Message>> first,
                            Iterable<Tuple2<Long, HashSet<PCVertex<K, VV, EV>>>> second,
                            Collector<Tuple2<Long, HashSet<PCVertex<K, VV, EV>>>> out) throws Exception {
            Iterator<Tuple2<Long, HashSet<PCVertex<K, VV, EV>>>> partitionIter = second.iterator();
            if (partitionIter.hasNext()) {
                Tuple2<Long, HashSet<PCVertex<K, VV, EV>>> partition = partitionIter.next();
                updateFunction.setPartitionId(partition.f0);
                updateFunction.setCollector(out);
                updateFunction.updatePartition(partition.f1, first);
            } else {
                throw new Exception("Invalid partition id");
            }
        }

        @Override
        public TypeInformation<Tuple2<Long, HashSet<PCVertex<K, VV, EV>>>> getProducedType() {
            return resultType;
        }
    }

    /**
     * Assign the partition id to each vertex
     * @param <K>
     * @param <VV>
     * @param <EV>
     */
    private static class PartitionMapFunction<K, VV, EV> extends
            RichFlatMapFunction<Tuple2<Long, HashSet<PCVertex<K, VV, EV>>>, Tuple2<K, Long>> implements
            ResultTypeQueryable<Tuple2<K, Long>> {
        private static final long serialVersionUID = 1L;

        private transient TypeInformation<Tuple2<K, Long>> resultType;

        public PartitionMapFunction(TypeInformation<Tuple2<K, Long>> resultType) {
            this.resultType = resultType;
        }

        @Override
        public void flatMap(Tuple2<Long, HashSet<PCVertex<K, VV, EV>>> value, Collector<Tuple2<K, Long>> out) throws Exception {
            for(PCVertex<K, VV, EV> vertex: value.f1) {
                out.collect(new Tuple2<>(vertex.getId(), value.f0));
            }
        }

        @Override
        public TypeInformation<Tuple2<K, Long>> getProducedType() {
            return resultType;
        }
    }
}
