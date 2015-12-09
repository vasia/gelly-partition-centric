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
 * Testing the PCSingleSourceShortestPaths on the Libimseti dating website dataset.
 */
public class Libimseti {

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
                .fromCsvReader(wrapper.getInputRoot() + "libimseti/out.libimseti.data", wrapper.getEnvironment())
                .ignoreInvalidLinesEdges()
                .fieldDelimiterEdges("\t")
                .lineDelimiterEdges("\n")
                .edgeTypes(Long.class, Double.class)
                .mapVertices(new MapFunction<Vertex<Long, NullValue>, Double>() {
                    @Override
                    public Double map(Vertex<Long, NullValue> value) throws Exception {
                        return Double.MAX_VALUE;
                    }
                });

        int i = 1;

        switch (args[0]) {
            case "pc":
                while (i > 0) {
                    GraphSSSPRunner.findSsspPC(
                            wrapper.getEnvironment(),
                            graph,
                            1L,
                            wrapper.getOutputRoot() + "pc_libimseti"
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
                            wrapper.getOutputRoot() + "vc_libimseti");
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
