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
import org.apache.flink.util.Collector;

import java.io.Serializable;

/**
 * Aggregate all message sent to one vertex into one message
 *
 * @param <K>
 * @param <Message>
 */
public abstract class VertexUpdateFunction<K, VV, Message, EV> implements Serializable{
    private Collector<PCVertex<K, VV, EV>> collector;
    protected PCVertex<K, VV, EV> vertex;

    public abstract void updateVertex(Iterable<Tuple2<K, Message>> message);

    public void setVertexValue(VV value) {
        vertex.setValue(value);
        collector.collect(vertex);
    }

    public void setCollector(Collector<PCVertex<K, VV, EV>> collector) {
        this.collector = collector;
    }

    public void setVertex(PCVertex<K, VV, EV> vertex) {
        this.vertex = vertex;
        this.vertex.setUpdated(false);
    }
}
