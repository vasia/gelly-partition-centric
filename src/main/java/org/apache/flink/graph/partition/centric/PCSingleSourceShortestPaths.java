package org.apache.flink.graph.partition.centric;

import org.apache.commons.collections.map.HashedMap;
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
 * @param <EV> Type of Edge value
 */
public class PCSingleSourceShortestPaths<K, EV> implements GraphAlgorithm<K, Double, EV, DataSet<Vertex<K, Double>>> {

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
    public DataSet<Vertex<K, Double>> run(Graph<K, Double, EV> graph) throws Exception {

        //Update vertex values to 0.0 for source vertex and Double.MAX_VALUE for the rest
        Graph<K, Double, EV> updatedGraph = graph.mapVertices(new DoubleValueVertexMapper<K>(getSrcVertexId()));

        //Convert graph to partition-centric graph
        PCGraph<K, Double, EV> pcGraph = new PCGraph<>(updatedGraph);

        Graph<K, Double, EV> result =
                pcGraph.runPartitionCentricIteration(
                        new SSSPPartitionProcessFunction<K, EV>(configuration.isTelemetryEnabled()),
                        new SSSPVertexUpdateFunction<K, EV>(),
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
     * @param <EV> Type of Edge Value
     */
    public static final class SSSPPartitionProcessFunction<K, EV>  extends PartitionProcessFunction<K, Double, Double, EV> {

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
        private Set<Tuple2<Double, Edge<K, EV>>> valueEdges;
        private Set<Vertex<K, Double>> unSettledVertices;
        private Map<K, Double> settledVertices;

        @Override
        public void processPartition(Iterable<Tuple2<Double, Edge<K, EV>>> vertices) throws Exception {
            valueEdges = new HashSet<Tuple2<Double, Edge<K, EV>>>();
            unSettledVertices = new HashSet<Vertex<K, Double>>();
            settledVertices = new HashMap<K, Double>();
            distance = new HashMap<K, Double>();

            Vertex<K, Double> minVertex = findMinimumValue(vertices.iterator());

            if (minVertex.getValue() < Double.MAX_VALUE) {

                unSettledVertices.add(minVertex);

                while (!unSettledVertices.isEmpty()) {
                    Vertex<K, Double> vertex = findSmallestValue(unSettledVertices);
                    settledVertices.put(vertex.getId(), vertex.getValue());
                    unSettledVertices.remove(vertex);
                    findNeighbours(vertex, valueEdges.iterator());
                }

                Histogram messageHistogram = context.getHistogram(MESSAGE_SENT_ITER_CTR);
                LongCounter messageCounter = context.getLongCounter(MESSAGE_SENT_CTR);
                Histogram vertexHistogram = context.getHistogram(ACTIVE_VER_ITER_CTR);

                //Send minimum path values to target vertices
                for (HashMap.Entry<K, Double> targetVertex : distance.entrySet()) {

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
        }

        private Vertex<K, Double> findMinimumValue(Iterator<Tuple2<Double, Edge<K, EV>>> vertices) {
            Vertex<K, Double> minVertex = new Vertex<>(null, Double.MAX_VALUE);
            while (vertices.hasNext()) {
                Tuple2<Double, Edge<K, EV>> vertex = vertices.next();
                valueEdges.add(vertex);
                if (vertex.f0 < minVertex.getValue()) {
                    Edge<K, EV> edge = vertex.f1;
                    minVertex.setId(edge.getSource());
                    minVertex.setValue(vertex.f0);
                }
            }

            return minVertex;
        }

        private Vertex<K, Double> findSmallestValue(Set<Vertex<K, Double>> vertices) {
            Vertex<K, Double> minVertex = new Vertex<>(null, Double.MAX_VALUE);
            for (Vertex<K, Double> vertex : vertices) {
                if (minVertex.getId() == null) {
                    minVertex = vertex;
                } else if (vertex.getValue() < minVertex.getValue()) {
                    minVertex = vertex;
                }
            }

            return minVertex;
        }

        private void findNeighbours(Vertex<K, Double> sourceVertex, Iterator<Tuple2<Double, Edge<K, EV>>> vertices) {

            while (vertices.hasNext()) {
                Tuple2<Double, Edge<K, EV>> vertex = vertices.next();
                Edge<K, EV> edge = vertex.f1;
                if (edge.getSource() == sourceVertex.getId()) {
                    Vertex<K, Double> neighbour = new Vertex<K, Double>(edge.getTarget(), vertex.f0 + (Double)edge.getValue());

                    if (!settledVertices.containsKey(neighbour.getId())) {
                        Double oldValue = distance.get(neighbour.getId());
                        oldValue = (oldValue == null)? Double.MAX_VALUE : oldValue;

                        if (oldValue > neighbour.getValue()) {
                            distance.put(neighbour.getId(), neighbour.getValue());
                            unSettledVertices.add(neighbour);
                        }
                    } else {
                        valueEdges.remove(vertex);
                    }
                }
            }
        }
    }

    /**
     * User Defined Partition messaging function
     *
     * @param <K> Type of vertex ID
     * @param <EV> Type of Edge Value
     */
    public static class SSSPVertexUpdateFunction<K, EV> extends VertexUpdateFunction<K, Double, Double, EV> {

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

