package org.apache.flink.graph.partition.centric;


import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

/**
 * Represent a partition centric graph.
 *
 * @param <K> The type of a vertex's id
 * @param <VV> The type of a vertex's value
 * @param <EV> The type of an edge's value
 */
public class PCGraph<K, VV, EV> {

    private final ExecutionEnvironment context;
    private final DataSet<PCVertex<K, VV, EV>> vertices;

    public PCGraph(DataSet<PCVertex<K, VV, EV>> vertices, ExecutionEnvironment context) {
        this.context = context;
        this.vertices = vertices;
    }
}
