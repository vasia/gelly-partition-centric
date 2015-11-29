package org.apache.flink.graph.partition.centric;

import org.apache.flink.api.common.accumulators.Histogram;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.utils.NullValueEdgeMapper;
import org.apache.flink.types.NullValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Single Source Shortest Path algorithm implementation using partition centric iterations
 *
 * @param <K> Type of Vertex ID
 * @param <EV> Type of Edge value
 */
public class PCSingleSourceShortestPath<K, EV> implements GraphAlgorithm<K, Long, EV, DataSet<Vertex<K, Long>>> {

    public static final String MESSAGE_SENT_CTR = "long:message_sent";
    public static final String MESSAGE_SENT_ITER_CTR = "histogram:message_sent_iter_ctr";
    public static final String ACTIVE_VER_ITER_CTR = "histogram:active_ver_iter_ctr";
    public static final String ITER_CTR = "long:iteration_counter";

    private K srcVertexId;
    private int maxIterations;
    private final PartitionCentricConfiguration configuration;

    /**
     * Single Source Shortest Path algorithm single argument constructor
     *
     * @param srcVertexId ID of the source vertex
     * @param maxIterations Maximum number of iterations of algorithm
     */
    public PCSingleSourceShortestPath(K srcVertexId, int maxIterations) {
        this.srcVertexId = srcVertexId;
        this.maxIterations = maxIterations;
        this.configuration = null;
    }

    /**
     * Single Source Shortest Path algorithm constructor
     *
     * @param srcVertexId ID of the source vertex
     * @param maxIterations Maximum number of iterations of algorithm
     * @param configuration PartitionCentricConfiguration object for iterations
     */
    public PCSingleSourceShortestPath(K srcVertexId, int maxIterations, PartitionCentricConfiguration configuration) {
        this.srcVertexId = srcVertexId;
        this.maxIterations = maxIterations;
        this.configuration = configuration;
    }

    /**
     * Run function for the partition centric SSSP algorithm
     *
     * @param graph The input graph
     * @return The updated graph vertices
     * @throws Exception
     */
    @Override
    public DataSet<Vertex<K, Long>> run(Graph<K, Long, EV> graph) throws Exception {
        PCGraph<K, Long, EV> pcGraph = new PCGraph<>(graph);

        Graph<K, Long, EV> result =
                pcGraph.runPartitionCentricIteration(
                        new SSSPPartitionProcessFunction<K, EV>(),
                        new SSSPVertexUpdateFunction<K, EV>(),
                        configuration, maxIterations);

        return result.getVertices();
    }

    /**
     * User Defined Partition update function
     *
     * @param <K> Type of Vertex ID
     * @param <EV> Type of Edge Value
     */
    public static final class SSSPPartitionProcessFunction<K, EV>  extends PartitionProcessFunction<K, Long, Long, EV> {

        private static final long serialVersionUID = 1L;
        private static final Logger LOG = LoggerFactory.getLogger(SSSPPartitionProcessFunction.class);

        @Override
        public void processPartition(Iterable<Tuple2<Long, Edge<K, EV>>> vertices) throws Exception {

            for (Tuple2<Long, Edge<K, EV>> vertice : vertices) {

            }

        }
    }

    /**
     * User Defined Partition messaging function
     *
     * @param <K> Type of vertex ID
     * @param <EV> Type of Edge Value
     */
    public static class SSSPVertexUpdateFunction<K, EV> extends VertexUpdateFunction<K, Long, Long, EV> {

        @Override
        public void updateVertex(Iterable<Tuple2<K, Long>> message) {
            Histogram vertexHistogram = context.getHistogram(ACTIVE_VER_ITER_CTR);
            Long minValue = vertex.getValue();

            //find the new minimal value (from current value and message values)
            for (Tuple2<K, Long> m : message) {
                if (minValue > m.f1)
                    minValue = m.f1;
            }

            //If vertex value bigger than minValue, update vertex value
            //and increase the Active Vertex counter for the next iteration
            if (minValue < vertex.getValue()) {
                vertex.setValue(minValue);

                if (vertexHistogram != null) {
                    vertexHistogram.add(context.getSuperstepNumber() + 1);
                }
            }
        }
    }
    
}

