package org.apache.flink.graph.partition.centric;


import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.HashSet;

/**
 * Users need to subclass this class and implement their partition processing method
 *
 * @param <K> The type of a vertex's id
 * @param <VV> The type of a vertex's value
 * @param <Message> The type of message to send
 * @param <EV> The type of an edge's value
 */
public abstract class PartitionUpdateFunction<K, VV, Message, EV> implements Serializable {
    private static final long serialVersionUID = 1L;
    protected int currentStep;
    protected Long partitionId;
    protected Collector<Tuple2<Long, HashSet<PCVertex<K, VV, EV>>>> collector;

    public void setCurrentStep(int currentStep) {
        this.currentStep = currentStep;
    }

    public void setPartitionId(Long partitionId) {
        this.partitionId = partitionId;
    }

    public void setCollector(Collector<Tuple2<Long, HashSet<PCVertex<K, VV, EV>>>> collector) {
        this.collector = collector;
    }

    public abstract void updatePartition(Iterable<PCVertex<K, VV, EV>> vertices, Iterable<Tuple3<Long, K, Message>> inMessages) throws Exception;
}
