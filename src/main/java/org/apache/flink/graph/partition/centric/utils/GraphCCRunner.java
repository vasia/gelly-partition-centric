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
import org.apache.flink.graph.vertex.centric.ConnectedComponents;
import org.apache.flink.graph.partition.centric.PCConnectedComponents;
import org.apache.flink.graph.partition.centric.PartitionCentricConfiguration;
import org.apache.flink.graph.partition.centric.PartitionCentricIteration;
import org.apache.flink.graph.vertex.centric.VertexCentricIteration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Utility class to run connected component algorithm on different graph
 */
public class GraphCCRunner {
    private final static Logger LOG = LoggerFactory.getLogger(GraphCCRunner.class);

    public static <K, EV> void detectComponentVC(
            ExecutionEnvironment environment,
            Graph<K, Long, EV> graph,
            String vertexCentricOutput,
            boolean discardOutput,
            int loopCount) throws Exception {
        LOG.debug("Preparing to run vertex centric iteration in {} loops", loopCount);
        for(int i = 1; i <= loopCount; i++) {
            LOG.debug("Loop {} starting", i);
            JobExecutionResult result;
            Map<String, String> fields = new HashMap<>();

            environment.startNewSession();
            ConnectedComponents<K, EV> vcAlgo = new ConnectedComponents<>(Integer.MAX_VALUE);
            if (discardOutput) {
                vcAlgo.run(graph).output(new DiscardingOutputFormat<Vertex<K, Long>>());
            } else {
                vcAlgo.run(graph).writeAsCsv(vertexCentricOutput, FileSystem.WriteMode.OVERWRITE);
            }
            result = environment.execute();
            fields.clear();
            fields.put(ConnectedComponents.MESSAGE_SENT_CTR, "Total messages sent");
            fields.put(ConnectedComponents.MESSAGE_SENT_ITER_CTR, "Messages sent");
            fields.put(ConnectedComponents.ACTIVE_VER_ITER_CTR, "Active vertices");
            fields.put(VertexCentricIteration.ITER_CTR, "Iteration count");
            fields.put(VertexCentricIteration.ITER_TIMER, "Elapse time");
            Telemetry.printTelemetry("Vertex centric", result, fields);
            LOG.debug("Loop {} ended", i);
        }
    }

    public static <K, EV> void detectComponentPC(ExecutionEnvironment environment,
                                                 Graph<K, Long, EV> graph,
                                                 String partitionCentricOutput,
                                                 boolean discardOutput,
                                                 int loopCount) throws Exception {
        LOG.debug("Preparing to run partition centric iteration in {} loops", loopCount);
        for (int i = 1; i <= loopCount; i++) {
            LOG.debug("Loop {} starting", i);
            JobExecutionResult result;
            PartitionCentricConfiguration configuration = new PartitionCentricConfiguration();
            configuration.setTelemetryEnabled(true);
            Map<String, String> fields = new HashMap<>();

            environment.startNewSession();
            PCConnectedComponents<K, EV> algo = new PCConnectedComponents<>(
                    Integer.MAX_VALUE, configuration);
            if (discardOutput) {
                algo.run(graph).output(new DiscardingOutputFormat<Vertex<K, Long>>());
            } else {
                algo.run(graph).writeAsCsv(partitionCentricOutput, FileSystem.WriteMode.OVERWRITE);
            }
            result = environment.execute();
            fields.put(PCConnectedComponents.MESSAGE_SENT_CTR, "Total messages sent");
            fields.put(PCConnectedComponents.MESSAGE_SENT_ITER_CTR, "Messages sent");
            fields.put(PCConnectedComponents.ACTIVE_VER_ITER_CTR, "Active vertices");
            fields.put(PartitionCentricIteration.ITER_CTR, "Iteration count");
            fields.put(PartitionCentricIteration.ITER_TIMER, "Elapse time");
            Telemetry.printTelemetry("Partition centric", result, fields);
            LOG.debug("Loop {} ended", i);
        }
    }
}
