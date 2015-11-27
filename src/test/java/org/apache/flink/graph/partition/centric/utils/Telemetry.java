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
import org.apache.flink.graph.partition.centric.PCConnectedComponents;

import java.util.Map;

/**
 * Utilities class for Telemetry purpose
 */
public class Telemetry {
    public static void printTelemetry(String name, JobExecutionResult result, Map<String, String> fields) {
        System.out.println(name);
        for(Map.Entry<String, String> field: fields.entrySet()) {
            System.out.printf("%s: %d%n",
                    field.getValue(),
                    result.<Long>getAccumulatorResult(field.getKey()));
        }
        System.out.printf("Execution time: %s ms%n", result.getNetRuntime());
    }
}
