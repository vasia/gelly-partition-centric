package org.apache.flink.graph.partition.centric;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.example.utils.SingleSourceShortestPathsData;
import org.apache.flink.graph.partition.centric.utils.GraphGenerator;
import org.junit.Assert;
import org.junit.Test;

import java.util.Comparator;
import java.util.List;

/**
 * Automated test for SSSP algorithm
 */
public class PCSingleSourceShortestPathsTest {

    @Test
    public void testRun() throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        environment.getConfig().disableSysoutLogging();

        Graph<Long, Double, Double> graph = GraphGenerator.generateSSSPGraph(environment);
        Long srcVertexId = SingleSourceShortestPathsData.SRC_VERTEX_ID;

        PCSingleSourceShortestPaths<Long, Double> algo = new PCSingleSourceShortestPaths<Long, Double>(srcVertexId, ((int) graph.numberOfVertices()));

        List<Tuple2<Long, Double>> result = algo.run(graph).map(
                new RichMapFunction<Vertex<Long, Double>, Tuple2<Long, Double>>() {
                    @Override
                    public Tuple2<Long, Double> map(Vertex<Long, Double> value) throws Exception {
                        return new Tuple2<>(value.getId(), value.getValue());
                    }
                }).collect();

        //Sort resulting vertices
        result.sort(new Comparator<Tuple2<Long, Double>>() {
            @Override
            public int compare(Tuple2<Long, Double> o1, Tuple2<Long, Double> o2) {
                return o1.f0.compareTo(o2.f0);
            }
        });

        //Generate result string to compare to default vertex-centric result
        String resultString = "";
        for (Tuple2<Long, Double> r : result) {
            resultString += r.f0 + "," + r.f1 + "\n";
        }
        resultString = resultString.trim();

        Assert.assertEquals("Resulting vertex values are wrong.", SingleSourceShortestPathsData.RESULTED_SINGLE_SOURCE_SHORTEST_PATHS, resultString);
    }
}
