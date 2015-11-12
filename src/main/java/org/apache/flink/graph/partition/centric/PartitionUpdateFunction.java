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


import org.apache.flink.util.Collector;

import java.io.Serializable;

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
    protected Collector<PCVertex<K, VV, EV>> collector;
    protected boolean updated;

    public void setCurrentStep(int currentStep) {
        this.currentStep = currentStep;
    }

    public void setCollector(Collector<PCVertex<K, VV, EV>> collector) {
        this.collector = collector;
    }

    public void setUpdated(boolean updated) {
        this.updated = updated;
    }

    /**
     * Call this to update a vertex
     *
     * @param vertex
     */
    protected void updateVertex(PCVertex<K, VV, EV> vertex) {
        collector.collect(vertex);
    }

    /**
     * Call this function to process the partition.
     * If the partition is updated, call updateVertex with the new collection of vertices
     *
     * @param inMessages The messages to the vertices of the partition
     * @throws Exception
     */
    public abstract void updateVertex(Iterable<PCVertex<K, VV, EV>> inMessages) throws Exception;
}
