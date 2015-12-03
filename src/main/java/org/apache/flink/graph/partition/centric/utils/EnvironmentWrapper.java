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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;

/**
 * Factory class to generate environment
 */
public class EnvironmentWrapper {
    private final String inputRoot;
    private final String outputRoot;
    private final ExecutionEnvironment environment;

    private EnvironmentWrapper(String inputRoot, String outputRoot, ExecutionEnvironment environment) {
        this.inputRoot = inputRoot;
        this.outputRoot = outputRoot;
        this.environment = environment;
    }

    public static EnvironmentWrapper newRemote() throws IOException {
        Properties properties = new Properties();
        properties.load(new InputStreamReader(new FileInputStream("server.properties")));
        String inputRoot = properties.getProperty("inputRoot");
        String outputRoot = properties.getProperty("outputRoot");
        String flinkMasterURI = properties.getProperty("flinkMasterURI");
        int flinkMasterPort = Integer.parseInt(properties.getProperty("flinkMasterPort"));
        int parallelism = Integer.parseInt(properties.getProperty("parallelism", "12"));
        Configuration configuration = new Configuration();
        configuration.setString("akka.framesize", "52428800b");
        ExecutionEnvironment environment = ExecutionEnvironment
                .createRemoteEnvironment(
                        flinkMasterURI,
                        flinkMasterPort,
                        configuration,
                        "target/gelly-partition-centric-1.0-SNAPSHOT-fatjar.jar"
                );
        environment.getConfig().setParallelism(parallelism);
        return new EnvironmentWrapper(inputRoot, outputRoot, environment);
    }

    public static EnvironmentWrapper newLocal() {
        return new EnvironmentWrapper("data/", "out/", ExecutionEnvironment.getExecutionEnvironment());
    }

    public String getInputRoot() {
        return inputRoot;
    }

    public String getOutputRoot() {
        return outputRoot;
    }

    public ExecutionEnvironment getEnvironment() {
        return environment;
    }
}
