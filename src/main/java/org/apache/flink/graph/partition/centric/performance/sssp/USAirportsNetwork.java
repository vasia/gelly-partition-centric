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

package org.apache.flink.graph.partition.centric.performance.sssp;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.partition.centric.utils.EnvironmentWrapper;
import org.apache.flink.graph.partition.centric.utils.GraphSSSPRunner;
import org.apache.flink.types.NullValue;

/**
 * Testing the PCSingleSourceShortestPaths on the US Airport Network dataset
 */
public class USAirportsNetwork {

    public static void main(String[] args) throws Exception {

        EnvironmentWrapper wrapper;
        if (args.length < 4) {
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
                .fromCsvReader(wrapper.getInputRoot() + "us_airport_network/us_airport_network.data", wrapper.getEnvironment())
                .ignoreCommentsEdges("%")
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

        int i = Integer.parseInt(args[3]);
        switch (args[0]) {
            case "pc":
                while (i > 0) {
                    GraphSSSPRunner.findSsspPC(
                            wrapper.getEnvironment(),
                            graph,
                            Long.parseLong(args[2]),
                            wrapper.getOutputRoot() + "pc_usairport"
                    );
                    i--;
                }
                break;
            case "vc":
                while (i > 0) {
                    GraphSSSPRunner.findSsspVC(
                            wrapper.getEnvironment(),
                            graph,
                            Long.parseLong(args[2]),
                            wrapper.getOutputRoot() + "vc_usairport");
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
        System.err.printf("Run \"java %s pc local [source_vertex_ID] [numbber_of_cycles]\" for local partition-centric%n", USAirportsNetwork.class);
        System.err.printf("Run \"java %s vc local [source_vertex_ID] [numbber_of_cycles]\" for local vertex-centric%n", USAirportsNetwork.class);
        System.err.printf("Run \"java %s pc remote [source_vertex_ID] [numbber_of_cycles]\" for remote partition-centric%n", USAirportsNetwork.class);
        System.err.printf("Run \"java %s pc remote [source_vertex_ID] [numbber_of_cycles]\" for remote partition-centric%n", USAirportsNetwork.class);
        System.exit(-1);
    }
}
