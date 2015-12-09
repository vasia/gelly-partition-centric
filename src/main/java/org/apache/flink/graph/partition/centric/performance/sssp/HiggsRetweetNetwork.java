package org.apache.flink.graph.partition.centric.performance.sssp;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.partition.centric.utils.EnvironmentWrapper;
import org.apache.flink.graph.partition.centric.utils.GraphSSSPRunner;
import org.apache.flink.types.NullValue;

/**
 * Created by nikola on 9.12.15.
 */
public class HiggsRetweetNetwork {


    public static void main(String[] args) throws Exception {

        EnvironmentWrapper wrapper;
        if (args.length < 3) {
            printErr();
            return;
        } else if (args[1].equals("remote")) {
            wrapper = EnvironmentWrapper.newRemote();
        } else if (args[1].equals("local")) {
            wrapper = EnvironmentWrapper.newLocal();
        } else {
            printErr();
            return;
        }

        wrapper.getEnvironment().getConfig().disableSysoutLogging();

        Graph<Long, Double, Double> graph = Graph
                .fromCsvReader(wrapper.getInputRoot() + "higgs-retweet_network/higgs-retweet_network.data", wrapper.getEnvironment())
                .ignoreInvalidLinesEdges()
                .fieldDelimiterEdges(" ")
                .lineDelimiterEdges("\n")
                .edgeTypes(Long.class, Double.class)
                .mapVertices(new MapFunction<Vertex<Long, NullValue>, Double>() {
                    @Override
                    public Double map(Vertex<Long, NullValue> value) throws Exception {
                        return Double.MAX_VALUE;
                    }
                });

        int i = 5;

        switch (args[0]) {
            case "pc":
                while (i > 0) {
                    GraphSSSPRunner.findSsspPC(
                            wrapper.getEnvironment(),
                            graph,
                            1L,
                            wrapper.getOutputRoot() + "pc_higgs-retweet_network"
                    );
                    i--;
                }
                break;
            case "vc":
                while (i > 0) {
                    GraphSSSPRunner.findSsspVC(
                            wrapper.getEnvironment(),
                            graph,
                            Long.getLong(args[2]),
                            wrapper.getOutputRoot() + "vc_higgs-retweet_network");
                    i--;
                }
                break;
            default:
                printErr();
                break;
        }
    }

    private static void printErr() {
        System.err.println("Please choose benchmark to run.");
        System.err.printf("Run \"java %s pc local\" for local partition-centric%n", Libimseti.class);
        System.err.printf("Run \"java %s vc local\" for local vertex-centric%n", Libimseti.class);
        System.err.printf("Run \"java %s pc remote\" for remote partition-centric%n", Libimseti.class);
        System.err.printf("Run \"java %s pc remote\" for remote partition-centric%n", Libimseti.class);
        System.exit(-1);
    }
}
