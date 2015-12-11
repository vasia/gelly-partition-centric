package org.apache.flink.graph.partition.centric;

import org.apache.flink.api.common.accumulators.Histogram;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;


/**
 * Single Source Shortest Path algorithm implementation using partition centric iterations
 *
 * @param <K> Type of Vertex ID
 */
public class PCSingleSourceShortestPaths<K> implements GraphAlgorithm<K, Double, Double, DataSet<Vertex<K, Double>>> {

    public static final String MESSAGE_SENT_CTR = "long:message_sent";
    public static final String MESSAGE_SENT_ITER_CTR = "histogram:message_sent_iter_ctr";
    public static final String ACTIVE_VER_ITER_CTR = "histogram:active_ver_iter_ctr";

    private K srcVertexId;
    private int maxIterations;
    private final PartitionCentricConfiguration configuration;

    /**
     * Creates an instance of the Single Source Shortest Path algorithm - single argument constructor
     *
     * @param srcVertexId ID of the source vertex
     * @param maxIterations Maximum number of iterations of algorithm
     */
    public PCSingleSourceShortestPaths(K srcVertexId, int maxIterations) {
        this.srcVertexId = srcVertexId;
        this.maxIterations = maxIterations;
        this.configuration = new PartitionCentricConfiguration();
    }

    /**
     * Creates an instance of Single Source Shortest Path algorithm
     *
     * @param srcVertexId ID of the source vertex
     * @param maxIterations Maximum number of iterations of algorithm
     * @param configuration PartitionCentricConfiguration object for iterations
     */
    public PCSingleSourceShortestPaths(K srcVertexId, int maxIterations, PartitionCentricConfiguration configuration) {
        this.srcVertexId = srcVertexId;
        this.maxIterations = maxIterations;
        this.configuration = configuration;
    }

    public K getSrcVertexId() {
        return srcVertexId;
    }

    /**
     * Run function for the partition centric SSSP algorithm
     *
     * @param graph The input graph
     * @return The updated graph vertices
     * @throws Exception
     */
    @Override
    public DataSet<Vertex<K, Double>> run(Graph<K, Double, Double> graph) throws Exception {

        //Update vertex values to 0.0 for source vertex and Double.MAX_VALUE for the rest
        Graph<K, Double, Double> updatedGraph = graph.mapVertices(new DoubleValueVertexMapper<K>(getSrcVertexId()));

        //Convert graph to partition-centric graph
        PCGraph<K, Double, Double> pcGraph = new PCGraph<>(updatedGraph);

        Graph<K, Double, Double> result =
                pcGraph.runPartitionCentricIteration(
                        new SSSPPartitionProcessFunction<K>(configuration.isTelemetryEnabled()),
                        new SSSPVertexUpdateFunction<K>(),
                        configuration, maxIterations);

        return result.getVertices();
    }

    public static final class DoubleValueVertexMapper<K> implements MapFunction<Vertex<K, Double>, Double> {
        private K srcVertexId;

        public DoubleValueVertexMapper(K srcVertexId) {
            this.srcVertexId = srcVertexId;
        }

        @Override
        public Double map(Vertex<K, Double> value) throws Exception {
            if (value.getId().equals(this.srcVertexId)) {
                return 0.0;
            }
            else {
                return Double.MAX_VALUE;
            }
        }
    }

    /**
     * User Defined Partition update function
     *
     * @param <K> Type of Vertex ID
     */
    public static final class SSSPPartitionProcessFunction<K>  extends PartitionProcessFunction<K, Double, Double, Double> {

        private static final long serialVersionUID = 1L;
        private static final Logger LOG = LoggerFactory.getLogger(SSSPPartitionProcessFunction.class);
        private final boolean telemetryEnabled;

        public SSSPPartitionProcessFunction() {
            telemetryEnabled = false;
        }

        public SSSPPartitionProcessFunction(boolean telemetryEnabled) {
            this.telemetryEnabled = telemetryEnabled;
        }

        @Override
        public void preSuperstep() {
            if (telemetryEnabled) {
                context.addAccumulator(MESSAGE_SENT_CTR, new LongCounter());
                context.addAccumulator(MESSAGE_SENT_ITER_CTR, new Histogram());
                context.addAccumulator(ACTIVE_VER_ITER_CTR, new Histogram());
            }
        }

        private Map<K, Double> distance;
        private Set<Tuple2<Double, Edge<K, Double>>> valueEdges;
        private Set<K> settledVertices;
        private PriorityQueue<Vertex<K, Double>> unSettledVertices;

        @Override
        public void processPartition(Iterable<Tuple2<Double, Edge<K, Double>>> vertices) throws Exception {
            valueEdges = new HashSet<>();
            settledVertices = new HashSet<>();
            distance = new HashMap<>();

            unSettledVertices = new PriorityQueue<>(new Comparator<Vertex<K, Double>>() {
                @Override
                public int compare(Vertex<K, Double> o1, Vertex<K, Double> o2) {
                    return o1.getValue().compareTo(o2.getValue());
                }
            });

            for (Tuple2<Double, Edge<K, Double>> vertex1 : vertices) {
                if (vertex1.f0 < Double.MAX_VALUE) {
                    distance.put(vertex1.f1.getSource(), vertex1.f0);
                    unSettledVertices.add(new Vertex<>(vertex1.f1.getSource(), vertex1.f0));
                }
                valueEdges.add(vertex1);
            }

            while (!unSettledVertices.isEmpty()) {
                Vertex<K, Double> vertex = unSettledVertices.poll();
                // Only visit the vertex once
                if (!settledVertices.contains(vertex.getId())) {
                    settledVertices.add(vertex.getId());
                    findNeighbours(vertex, valueEdges.iterator());
                }
            }

            Histogram messageHistogram = context.getHistogram(MESSAGE_SENT_ITER_CTR);
            LongCounter messageCounter = context.getLongCounter(MESSAGE_SENT_CTR);
            Histogram vertexHistogram = context.getHistogram(ACTIVE_VER_ITER_CTR);

            //Send minimum path values to target vertices
            for (Map.Entry<K, Double> targetVertex : distance.entrySet()) {

                sendMessage(targetVertex.getKey(), targetVertex.getValue());

                if (messageCounter != null) {
                    messageCounter.add(1);
                }
                if (messageHistogram != null) {
                    messageHistogram.add(context.getSuperstepNumber());
                }

                //Since a vertex can send multiple messages in one iteration,    && !partitionVertices.contains(targetVertice.getKey())
                //we need to count only the local receiving vertices
                if (vertexHistogram != null) {
                    vertexHistogram.add(context.getSuperstepNumber());
                }
            }
        }

        private void findNeighbours(Vertex<K, Double> sourceVertex, Iterator<Tuple2<Double, Edge<K, Double>>> vertices) {

            while (vertices.hasNext()) {
                Tuple2<Double, Edge<K, Double>> vertex = vertices.next();
                Edge<K, Double> edge = vertex.f1;
                if (edge.getSource().equals(sourceVertex.getId())) {
                    Vertex<K, Double> neighbour = new Vertex<>(edge.getTarget(), vertex.f0 + edge.getValue());
                    if (!settledVertices.contains(neighbour.getId())) {
                        Double oldValue = distance.get(neighbour.getId());
                        if (oldValue == null || oldValue > neighbour.getValue()) {
                            distance.put(neighbour.getId(), neighbour.getValue());
                        }
                        unSettledVertices.add(neighbour);
                    }
                }
            }
        }
    }

    /**
     * User Defined Partition messaging function
     *
     * @param <K> Type of vertex ID
     */
    public static class SSSPVertexUpdateFunction<K> extends VertexUpdateFunction<K, Double, Double, Double> {

        @Override
        public void updateVertex(Iterable<Tuple2<K, Double>> message) {
            Histogram vertexHistogram = context.getHistogram(ACTIVE_VER_ITER_CTR);
            Double minValue = vertex.getValue();

            //find the new minimal value (from current value and message values)
            for (Tuple2<K, Double> m : message) {
                if (minValue > m.f1)
                    minValue = m.f1;
            }

            //If vertex value bigger than minValue, update vertex value
            //and increase the Active Vertex counter for the next iteration
            if (minValue < vertex.getValue()) {
                setVertexValue(minValue);

                if (vertexHistogram != null) {
                    vertexHistogram.add(context.getSuperstepNumber() + 1);
                }
            }
        }
    }

}

