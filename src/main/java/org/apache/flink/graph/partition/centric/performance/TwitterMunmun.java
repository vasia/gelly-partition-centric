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
import org.apache.flink.graph.partition.centric.utils.EnvironmentWrapper;
import org.apache.flink.graph.partition.centric.utils.GraphCCRunner;
import org.apache.flink.types.NullValue;

/**
 * Testing the PCConnectedComponents on Twitter Munmun dataset
 */
public class TwitterMunmun {
    public static void main(String[] args) throws Exception {

        EnvironmentWrapper wrapper;
        if (args.length < 2) {
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

        Graph<Long, Long, NullValue> graph = Graph
                .fromCsvReader(
                        wrapper.getInputRoot() + "munmun_twitter_social/out.munmun_twitter_social.data",
                        new MapFunction<Object, Object>() {
                            @Override
                            public Object map(Object value) throws Exception {
                                return value;
                            }
                        }, wrapper.getEnvironment())
                .fieldDelimiterEdges(" ")
                .lineDelimiterEdges("\n")
                .ignoreCommentsEdges("%")
                .vertexTypes(Long.class, Long.class);

        switch (args[0]) {
            case "pc":
                GraphCCRunner.detectComponentPC(
                        wrapper.getEnvironment(),
                        graph,
                        wrapper.getOutputRoot() + "pctwitter"
                );
                break;
            case "vc":
                GraphCCRunner.detectComponentVC(
                        wrapper.getEnvironment(),
                        graph,
                        wrapper.getOutputRoot() + "vctwitter");
                break;
            default:
                printErr();
                break;
        }
    }

    private static void printErr() {
        System.err.println("Please choose benchmark to run.");
        System.err.printf("Run \"java %s pc local\" for local partition-centric%n", TwitterMunmun.class);
        System.err.printf("Run \"java %s vc local\" for local vertex-centric%n", TwitterMunmun.class);
        System.err.printf("Run \"java %s pc remote\" for remote partition-centric%n", TwitterMunmun.class);
        System.err.printf("Run \"java %s pc remote\" for remote partition-centric%n", TwitterMunmun.class);
        System.exit(-1);
    }
}
