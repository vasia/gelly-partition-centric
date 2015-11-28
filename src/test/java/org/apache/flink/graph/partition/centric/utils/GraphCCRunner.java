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
import org.apache.flink.api.common.accumulators.Histogram;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.library.ConnectedComponents;
import org.apache.flink.graph.partition.centric.PCConnectedComponents;
import org.apache.flink.graph.partition.centric.PartitionCentricConfiguration;
import org.apache.flink.graph.partition.centric.PartitionCentricIteration;
import org.apache.flink.types.NullValue;

import java.util.HashMap;
import java.util.Map;

/**
 * Utility class to run connected component algorithm on different graph
 */
public class GraphCCRunner {
    public static void detectComponent(
            ExecutionEnvironment environment,
            Graph<Long, Long, NullValue> graph,
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
        PCConnectedComponents<Long, NullValue> algo = new PCConnectedComponents<>(
                Integer.MAX_VALUE, configuration);
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
        ConnectedComponents<Long, NullValue> vcAlgo = new ConnectedComponents<>(Integer.MAX_VALUE);
        vcAlgo.run(graph).writeAsCsv(vertexCentricOutput, FileSystem.WriteMode.OVERWRITE);
        result = environment.execute();
        fields.clear();
        Telemetry.printTelemetry("Vertex centric", result, fields);
    }
}
