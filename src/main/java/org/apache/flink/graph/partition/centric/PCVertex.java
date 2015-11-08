package org.apache.flink.graph.partition.centric;

import org.apache.flink.api.java.tuple.Tuple3;

import java.util.HashMap;

/**
 * Represents a partition centric graph's nodes.
 * It carries the vertex id, vertex value and all outgoing edges
 * For vertices or edges with no value, use {@link org.apache.flink.types.NullValue} as the value type.
 *
 * @param <K> The type of a vertex's id
 * @param <VV> The type of a vertex's value
 * @param <EV> The type of an edge's value
 */
public class PCVertex<K, VV, EV> extends Tuple3<K, VV, HashMap<K, EV>> {
    private static final long serialVersionUID = 1L;

    public PCVertex() {
        this.f2 = new HashMap<>();
    }

    public PCVertex(K id, VV value, HashMap<K, EV> edges) {
        this.f0 = id;
        this.f1 = value;
        this.f2 = edges;
    }

    public K getId() {
        return f0;
    }

    public void setId(K id) {
        this.f0 = id;
    }

    public VV getValue() {
        return f1;
    }

    public void setValue(VV value) {
        this.f1 = value;
    }

    public HashMap<K, EV> getEdges() {
        return f2;
    }

    public void setEdges(HashMap<K, EV> edges) {
        this.f2 = edges;
    }

    public EV putEdge(K target, EV value) {
        return f2.put(target, value);
    }

    public EV removeEdge(K target) {
        return f2.remove(target);
    }

}
