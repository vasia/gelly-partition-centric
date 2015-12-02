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
