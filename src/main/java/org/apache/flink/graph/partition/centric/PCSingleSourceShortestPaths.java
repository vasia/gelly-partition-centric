package org.apache.flink.graph.partition.centric;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;


/**
 * Single Source Shortest Path algorithm implementation using partition centric iterations
 *
 * @param <K> Type of Vertex ID
 */
public class PCSingleSourceShortestPaths<K> implements GraphAlgorithm<K, Double, Double, DataSet<Vertex<K, Double>>> {

    private K srcVertexId;
    private int maxIterations;

    /**
     * Creates an instance of the Single Source Shortest Path algorithm - single argument constructor
     *
     * @param srcVertexId ID of the source vertex
     * @param maxIterations Maximum number of iterations of algorithm
     */
    public PCSingleSourceShortestPaths(K srcVertexId, int maxIterations) {
        this.srcVertexId = srcVertexId;
        this.maxIterations = maxIterations;
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
                        new SSSPPartitionProcessFunction<K>(),
                        new SSSPVertexUpdateFunction<K>(),
                        maxIterations);

        return result.getVertices();
    }

    @SuppressWarnings("serial")
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
    public static final class SSSPPartitionProcessFunction<K> extends PartitionProcessFunction<K, Double, Double, Double> {

        private static final long serialVersionUID = 1L;

        private Map<K, Double> distance;
        private Set<K> settledVertices;
        private PriorityQueue<Vertex<K, Double>> unSettledVertices;
        private Map<K, ArrayList<Tuple2<K, Double>>> vertexNeighbours;

        @Override
        public void processPartition(Iterable<RichEdge<K, Double, Double>> edges) throws Exception {

            settledVertices = new HashSet<>();
            distance = new HashMap<>();
            vertexNeighbours = new HashMap<>();

            unSettledVertices = new PriorityQueue<>(100, new Comparator<Vertex<K, Double>>() {
                @Override
                public int compare(Vertex<K, Double> o1, Vertex<K, Double> o2) {
                    return o1.getValue().compareTo(o2.getValue());
                }
            });

            for (RichEdge<K, Double, Double> edge : edges) {
                if (edge.getSourceValue() < Double.MAX_VALUE) {
                    distance.put(edge.getSourceId(), edge.getSourceValue());
                    unSettledVertices.add(new Vertex<>(edge.getSourceId(), edge.getSourceValue()));
                }
                if (vertexNeighbours.containsKey(edge.getSourceId())) {
                    vertexNeighbours.get(edge.getSourceId()).add(new Tuple2<K, Double>(
                    		edge.getTargetId(), edge.getEdgeValue()));
                }
                else {
                    ArrayList<Tuple2<K, Double>> tmp = new ArrayList<>();
                    tmp.add(new Tuple2<K, Double>(edge.getTargetId(), edge.getEdgeValue()));
                    vertexNeighbours.put(edge.getSourceId(), tmp);
                }
            }

            while (!unSettledVertices.isEmpty()) {
                Vertex<K, Double> vertex = unSettledVertices.poll();
                // Only visit the vertex once
                if (!settledVertices.contains(vertex.getId())) {
                    settledVertices.add(vertex.getId());
                    ArrayList<Tuple2<K, Double>> currentNeighbours = vertexNeighbours.get(vertex.getId());
                    if (currentNeighbours != null)
                        findNeighbours(vertex, currentNeighbours.iterator());
                }
            }
        }

        private void findNeighbours(Vertex<K, Double> sourceVertex, Iterator<Tuple2<K, Double>> vertices) {

            while (vertices.hasNext()) {
                Tuple2<K, Double> vertex = vertices.next();
                Vertex<K, Double> neighbour = new Vertex<>(vertex.f0, sourceVertex.getValue() + vertex.f1);
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

    /**
     * User Defined Partition messaging function
     *
     * @param <K> Type of vertex ID
     */
    @SuppressWarnings("serial")
	public static class SSSPVertexUpdateFunction<K> extends VertexUpdateFunction<K, Double, Double, Double> {

        @Override
        public void updateVertex(Vertex<K, Double> vertex, MessageIterator<Double> messages) {

            Double minValue = vertex.getValue();

            //find the new minimal value (from current value and message values)
            for (double m : messages) {
                if (minValue > m)
                    minValue = m;
            }

            //If vertex value bigger than minValue, update vertex value
            //and increase the Active Vertex counter for the next iteration
            if (minValue < vertex.getValue()) {
                setNewVertexValue(minValue);
            }
        }
    }
}
