package org.apache.flink.graph.partition.centric;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;

/**
 * Represent a partition centric graph.
 *
 * @param <K>  The type of a vertex's id
 * @param <VV> The type of a vertex's value
 * @param <EV> The type of an edge's value
 */
public class PCGraph<K, VV, EV> {

    private final DataSet<PCVertex<K, VV, EV>> vertices;

    private PCGraph(DataSet<PCVertex<K, VV, EV>> vertices) {
        this.vertices = vertices;
    }

    public DataSet<PCVertex<K, VV, EV>> getVertices() {
        return vertices;
    }

    public static <K, VV, EV> PCGraph<K, VV, EV> fromGraph(final Graph<K, VV, EV> graph) {
        DataSet<Vertex<K, VV>> graphVertices = graph.getVertices();
        final DataSet<Edge<K, EV>> graphEdges = graph.getEdges();

        DataSet<PCVertex<K, VV, EV>> pcVertices = graphVertices.coGroup(graphEdges).where(0).equalTo(0).with(
                new CoGroupFunction<Vertex<K, VV>, Edge<K, EV>, PCVertex<K, VV, EV>>() {
                    @Override
                    public void coGroup(
                            Iterable<Vertex<K, VV>> first,
                            Iterable<Edge<K, EV>> second,
                            Collector<PCVertex<K, VV, EV>> out) throws Exception {
                        PCVertex<K, VV, EV> pcVertex = new PCVertex<>();
                        for (Vertex<K, VV> v : first) {
                            pcVertex.setId(v.getId());
                            pcVertex.setValue(v.getValue());
                        }
                        for (Edge<K, EV> e : second) {
                            pcVertex.putEdge(e.getTarget(), e.getValue());
                        }
                        out.collect(pcVertex);
                    }
                }
        );
        return new PCGraph<>(pcVertices);
    }

    public<Message> PCGraph<K, VV, EV> runPartitionCentricIteration(
            PartitionUpdateFunction<K, VV, Message, EV> updateFunction,
            PartitionMessagingFunction<K, VV, Message, EV> messagingFunction,
            int maximumNumOperations) {
        PartitionCentricIteration<K, VV, Message, EV> iteration = new PartitionCentricIteration<>(
                updateFunction, messagingFunction, maximumNumOperations);

        DataSet<PCVertex<K, VV, EV>> updatedVertices = vertices.runOperation(iteration);

        return new PCGraph<>(updatedVertices);
    }
}
