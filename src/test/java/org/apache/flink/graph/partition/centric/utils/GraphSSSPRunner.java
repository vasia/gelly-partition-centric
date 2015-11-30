package org.apache.flink.graph.partition.centric.utils;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.Histogram;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.library.ConnectedComponents;
import org.apache.flink.graph.library.SingleSourceShortestPaths;
import org.apache.flink.graph.partition.centric.PCConnectedComponents;
import org.apache.flink.graph.partition.centric.PCSingleSourceShortestPaths;
import org.apache.flink.graph.partition.centric.PartitionCentricConfiguration;
import org.apache.flink.graph.partition.centric.PartitionCentricIteration;
import org.apache.flink.types.NullValue;

import java.util.HashMap;
import java.util.Map;

/**
 * Utility class to run SSSP algorithm on different graphs
 */
public class GraphSSSPRunner {

    public static void detectComponent(
            ExecutionEnvironment environment,
            Graph<Long, Double, Double> graph,
            Long srcVertexId,
            String partitionCentricOutput,
            String vertexCentricOutput) throws Exception {

        JobExecutionResult result;

        PartitionCentricConfiguration configuration = new PartitionCentricConfiguration();

        configuration.registerAccumulator(PCConnectedComponents.MESSAGE_SENT_CTR, new LongCounter());
        configuration.registerAccumulator(PCConnectedComponents.MESSAGE_SENT_ITER_CTR, new Histogram());
        configuration.registerAccumulator(PCConnectedComponents.ITER_CTR, new LongCounter());
        configuration.registerAccumulator(PCConnectedComponents.ACTIVE_VER_ITER_CTR, new Histogram());
        configuration.registerAccumulator(PartitionCentricIteration.ITER_TIMER, new IterationTimer());

        environment.startNewSession();

        PCSingleSourceShortestPaths<Long, Double> algo = new PCSingleSourceShortestPaths<Long, Double>(srcVertexId, Integer.MAX_VALUE, configuration);

        algo.run(graph).writeAsCsv(partitionCentricOutput, FileSystem.WriteMode.OVERWRITE);
        result = environment.execute();
        Map<String, String> fields = new HashMap<>();

        fields.put(PCConnectedComponents.MESSAGE_SENT_CTR, "Total messages sent");
        fields.put(PCConnectedComponents.MESSAGE_SENT_ITER_CTR, "Messages sent");
        fields.put(PCConnectedComponents.ITER_CTR, "Iteration count");
        fields.put(PCConnectedComponents.ACTIVE_VER_ITER_CTR, "Active vertices");
        fields.put(PartitionCentricIteration.ITER_TIMER, "Elapse time");

        Telemetry.printTelemetry("Partition centric", result, fields);

        environment.startNewSession();
        SingleSourceShortestPaths<Long> vcAlgo = new SingleSourceShortestPaths<>(srcVertexId, Integer.MAX_VALUE);
        vcAlgo.run(graph).writeAsCsv(vertexCentricOutput, FileSystem.WriteMode.OVERWRITE);
        result = environment.execute();
        fields.clear();
        Telemetry.printTelemetry("Vertex centric", result, fields);
    }
}
