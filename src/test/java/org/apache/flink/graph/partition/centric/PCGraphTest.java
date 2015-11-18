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

package org.apache.flink.graph.partition.centric;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Unit test for PCGraph
 */
public class PCGraphTest {

//    @Test
//    public void testFromGraph() throws Exception {
//        Collection<Vertex<Integer, Double>> vertices = Arrays.asList(
//                new Vertex<>(1, 2.0),
//                new Vertex<>(2, 3.0),
//                new Vertex<>(3, 2.0),
//                new Vertex<>(4, 2.0)
//        );
//        Collection<Edge<Integer, Double>> edges = Arrays.asList(
//                new Edge<>(1, 2, 1.0),
//                new Edge<>(2, 3, 1.0),
//                new Edge<>(3, 1, 3.0),
//                new Edge<>(3, 2, 2.0),
//                new Edge<>(3, 4, 1.0)
//        );
//
//        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
//
//        Graph<Integer, Double, Double> graph = Graph.fromCollection(vertices, edges, environment);
//
//        PCGraph<Integer, Double, Double> pcGraph = PCGraph.fromGraph(graph);
//
//        DataSet<PCVertex<Integer, Double, Double>> pcVertices = pcGraph.getVertices();
//
//        List<PCVertex<Integer, Double, Double>> v3s =
//                pcVertices.filter(new FilterFunction<PCVertex<Integer, Double, Double>>() {
//                    @Override
//                    public boolean filter(PCVertex<Integer, Double, Double> value) throws Exception {
//                        return value.getId().equals(3);
//                    }
//                }).collect();
//
//        assertEquals(1, v3s.size());
//        PCVertex<Integer, Double, Double> v3 = v3s.get(0);
//        assertEquals(3, v3.getId().intValue());
//        assertEquals(2.0, v3.getValue(), 0.0001);
//        Map<Integer, Double> outEdges = v3.getEdges();
//        assertEquals(3, outEdges.size());
//        for (Map.Entry<Integer, Double> e : outEdges.entrySet()) {
//            if (e.getKey() == 1) {
//                assertEquals(3, e.getValue(), 0.0001);
//            } else if (e.getKey() == 2) {
//                assertEquals(2, e.getValue(), 0.0001);
//            } else if (e.getKey() == 4) {
//                assertEquals(1, e.getValue(), 0.0001);
//            }
//        }
//    }
}