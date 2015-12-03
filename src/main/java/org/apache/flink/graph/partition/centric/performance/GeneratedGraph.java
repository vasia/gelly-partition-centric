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

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.partition.centric.utils.GraphCCRunner;
import org.apache.flink.graph.partition.centric.utils.GraphGenerator;

/**
 * Compare performance between vertex centric and partition centric implementation
 * of ConnectedComponents algorithm
 */
public class GeneratedGraph {

    public static void main(String[] args) throws Exception {
        int verticesCount = 40000;
        int edgesCount = verticesCount * 20;

        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        environment.getConfig().disableSysoutLogging();
        Graph<Long, Long, Long> graph = GraphGenerator.generateGraph(verticesCount, edgesCount, environment);

        if (args.length < 1) {
            printErr();
        } else if (args[0].equals("1")) {
            GraphCCRunner.detectComponentPC(environment, graph, "out/pcgenerated", true, 1);
        } else if (args[0].equals("2")) {
            GraphCCRunner.detectComponentVC(environment, graph, "out/vcgenerated", true, 1);
        } else {
            printErr();
        }
    }

    private static void printErr() {
        System.err.println("Please choose benchmark to run.");
        System.err.printf("Run \"java %s 1\" for partition-centric%n", GeneratedGraph.class);
        System.err.printf("Run \"java %s 2\" for vertex-centric%n", GeneratedGraph.class);
        System.exit(-1);
    }
}
