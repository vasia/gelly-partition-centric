/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.graph.partition.centric.utils;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.partition.centric.PCSingleSourceShortestPaths;
import org.apache.flink.graph.partition.centric.PartitionCentricConfiguration;
import org.apache.flink.graph.partition.centric.PartitionCentricIteration;
import org.apache.flink.graph.vertex.centric.SingleSourceShortestPaths;
import org.apache.flink.graph.vertex.centric.VertexCentricIteration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Utility class to run SSSP algorithm on different graphs
 */
public class GraphSSSPRunner {

    private final static Logger LOG = LoggerFactory.getLogger(GraphSSSPRunner.class);

    public static void findSsspPC(
            ExecutionEnvironment environment,
            Graph<Long, Double, Double> graph,
            Long srcVertexId,
            String partitionCentricOutput) throws Exception {

        boolean discardResult = false;
        Map<String, String> fields = new HashMap<>();
        PartitionCentricConfiguration configuration = new PartitionCentricConfiguration();
        configuration.setTelemetryEnabled(true);

        environment.startNewSession();

        PCSingleSourceShortestPaths<Long> pcAlgorithm = new PCSingleSourceShortestPaths<Long>(srcVertexId, Integer.MAX_VALUE, configuration);

        if (discardResult) {
            pcAlgorithm.run(graph).output(new DiscardingOutputFormat<Vertex<Long, Double>>());
        } else {
            pcAlgorithm.run(graph).writeAsCsv(partitionCentricOutput, FileSystem.WriteMode.OVERWRITE);
        }
        List<JobExecutionResult> results = new ArrayList<>();
        results.add(environment.execute());


        fields.put(PCSingleSourceShortestPaths.MESSAGE_SENT_CTR, "Total messages sent");
        fields.put(PCSingleSourceShortestPaths.MESSAGE_SENT_ITER_CTR, "Messages sent");
        fields.put(PCSingleSourceShortestPaths.ACTIVE_VER_ITER_CTR, "Active vertices");
        fields.put(PartitionCentricIteration.ITER_CTR, "Iteration count");
        fields.put(PartitionCentricIteration.ITER_TIMER, "Elapse time");

        Telemetry.printTelemetry("Partition centric", results, fields);
    }

    public static void findSsspVC(ExecutionEnvironment environment,
                                  Graph<Long, Double, Double> graph,
                                  Long srcVertexId,
                                  String vertexCentricOutput) throws Exception {

        Map<String, String> fields = new HashMap<>();
        environment.startNewSession();

        SingleSourceShortestPaths<Long> vcAlgorithm = new SingleSourceShortestPaths<>(srcVertexId, Integer.MAX_VALUE);

        vcAlgorithm.run(graph).writeAsCsv(vertexCentricOutput, FileSystem.WriteMode.OVERWRITE);
        List<JobExecutionResult> results = new ArrayList<>();
        results.add(environment.execute());

        fields.put(SingleSourceShortestPaths.MESSAGE_SENT_CTR, "Total messages sent");
        fields.put(SingleSourceShortestPaths.MESSAGE_SENT_ITER_CTR, "Messages sent");
        fields.put(SingleSourceShortestPaths.ACTIVE_VER_ITER_CTR, "Active vertices");
        fields.put(VertexCentricIteration.ITER_CTR, "Iteration count");
        fields.put(VertexCentricIteration.ITER_TIMER, "Elapse time");

        Telemetry.printTelemetry("Vertex centric", results, fields);
    }
}
