package org.apache.flink.graph.partition.centric;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Map;

/**
 * User must override this class to send message to other partitions
 * @param <K>
 * @param <VV>
 * @param <Message>
 * @param <EV>
 */
public abstract class PartitionMessagingFunction<K, VV, Message, EV> implements Serializable {
    private static final long serialVersionUID = 1L;
    private Tuple3<Long, K, Message> outValue;
    private Collector<Tuple3<Long, K, Message>> collector;
    private Map<K, Long> partitionMap;
    protected HashSet<PCVertex<K, VV, EV>> sourcePartition;
    protected Long partitionId;
    protected int currentStep;

    /**
     * Implement this method to send message.
     * The source vertex is accessible through the private attribute sourceVertex
     */
    public abstract void sendMessages();

    public void init(Map<K, Long> partitionMap) {
        this.partitionMap = partitionMap;
        this.outValue = new Tuple3<>();
    }

    /**
     * Sends the given message to the vertex identified by the given key.
     * If the target vertex does not exist the next superstep will cause an exception
     * due to a non-deliverable message.
     *
     * @param target The key (id) of the target vertex to message.
     * @param m The message.
     */
    public void sendMessageTo(K target, Message m) {
        outValue.f0 = partitionMap.get(target);
        outValue.f1 = target;
        outValue.f2 = m;
        collector.collect(outValue);
    }

    public void setCollector(Collector<Tuple3<Long, K, Message>> collector) {
        this.collector = collector;
    }

    public void setSourcePartition(HashSet<PCVertex<K, VV, EV>> sourcePartition) {
        this.sourcePartition = sourcePartition;
    }

    public void setCurrentStep(int currentStep) {
        this.currentStep = currentStep;
    }

    /**
     * Check if the target is in the same partition with the source vertex.
     * A vertex should not send message to another vertex of the same partition.
     *
     * @param target The id of target vertex.
     * @return true if the target is in the same partition as the source vertex
     */
    public boolean isSamePartition(K target) {
        return partitionId.equals(partitionMap.get(target));
    }

    public void setPartitionId(Long partitionId) {
        this.partitionId = partitionId;
    }
}
