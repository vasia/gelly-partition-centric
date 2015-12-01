package org.apache.flink.graph.partition.centric.performance;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.partition.centric.utils.GraphSSSPRunner;
import org.apache.flink.types.NullValue;

/**
 * Testing the PCSingleSourceShortestPaths on the Libimseti dating website dataset.
 */
public class Libimseti {
    public static void main(String[] args) throws Exception {

        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        environment.getConfig().disableSysoutLogging();

        Graph<Long, Double, Double> graph = Graph
                .fromCsvReader("data/libimseti/out.libimseti.data", environment)
                .fieldDelimiterEdges("\t")
                .lineDelimiterEdges("\n")
                .ignoreCommentsEdges("%")
                .ignoreInvalidLinesEdges()
                .edgeTypes(Long.class, Double.class)
                .mapVertices(new MapFunction<Vertex<Long, NullValue>, Double>() {
                    @Override
                    public Double map(Vertex<Long, NullValue> value) throws Exception {
                        return Double.MAX_VALUE;
                    }
                });

        GraphSSSPRunner.detectComponent(environment, graph, 1l, "out/pc_libimseti", "out/vc_libimseti");
    }
}
