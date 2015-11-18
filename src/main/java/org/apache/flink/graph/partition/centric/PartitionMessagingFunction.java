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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * User must override this class to send message to other partitions
 * @param <K>
 * @param <VV>
 * @param <Message>
 * @param <EV>
 */
public abstract class PartitionMessagingFunction<K, VV, Message, EV> implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(PartitionMessagingFunction.class);

    private Tuple2<K, Message> outValue;
    private Collector<Tuple2<K, Message>> collector;
    protected int currentStep;
    protected Vertex<K, VV> sourceVertex;

    /**
     * Implement this method to send message.
     * The source vertex is accessible through the private attribute sourceVertex
     */
    public abstract void sendMessages(K[] external);

    public void init() {
        this.outValue = new Tuple2<>();
    }

    /**
     * Sends the given message to the vertex identified by the given key.
     * If the target vertex does not exist the next superstep will cause an exception
     * due to a non-deliverable message.
     *
     * @param target The key (id) of the target vertex to message.
     * @param m The message.
     */
    public void sendMessageTo(K target, Message m) {
        outValue.f0 = target;
        outValue.f1 = m;
        collector.collect(outValue);
    }

    public void setCollector(Collector<Tuple2<K, Message>> collector) {
        this.collector = collector;
    }

    public void setSourceVertex(Vertex<K, VV> sourceVertex) {
        this.sourceVertex = sourceVertex;
    }

    public void setCurrentStep(int currentStep) {
        this.currentStep = currentStep;
    }

}
