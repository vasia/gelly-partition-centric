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
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.HashMap;

/**
 * Users need to subclass this class and implement their partition processing method
 *
 * @param <K> The type of a vertex's id
 * @param <VV> The type of a vertex's value
 * @param <Message> The type of message to send
 * @param <EV> The type of an edge's value
 */
public abstract class PartitionUpdateFunction<K, VV, Message, EV> implements Serializable {
    private static final long serialVersionUID = 1L;
    protected int currentStep;
    protected Collector<Tuple2<K, Message>> collector;
    protected boolean updated;

    public void init() {
    }

    public void setCurrentStep(int currentStep) {
        this.currentStep = currentStep;
    }

    public void setCollector(Collector<Tuple2<K, Message>> collector) {
        this.collector = collector;
    }

    public void setUpdated(boolean updated) {
        this.updated = updated;
    }

    /**
     * Call this method to send a message to a vertex
     * @param vertex The destination vertex's id
     * @param message The message content
     */
    protected void sendMessage(K vertex, Message message) {
        collector.collect(new Tuple2<>(vertex, message));
    }

    /**
     * Call this function to process the partition.
     * If the partition is updated, call updateVertex with the new collection of vertices
     *
     * @param inMessages The messages to the vertices of the partition
     * @throws Exception
     */
    public abstract void updateVertex(Iterable<Tuple2<Vertex<K, VV>, HashMap<K, EV>>> inMessages) throws Exception;
}
