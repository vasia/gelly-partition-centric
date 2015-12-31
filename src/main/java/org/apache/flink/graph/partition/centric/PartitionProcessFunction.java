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

import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.io.Serializable;

/**
 * This class must be extended by functions that compute the state of the vertex
 * inside each partition.
 * <p>
 * The central method is {@link #processPartition(Iterable)}, which is
 * invoked once per partition per superstep.
 *
 * @param <K> The vertex ID type
 * @param <VV> The vertex value type
 * @param <Message> The message type
 * @param <EV> The edge value type
 */
public abstract class PartitionProcessFunction<K, VV, Message, EV> implements Serializable {
    private static final long serialVersionUID = 1L;
    protected Collector<Tuple2<K, Message>> collector;
    protected transient IterationRuntimeContext context;

    public void init(IterationRuntimeContext context) {
        this.context = context;
    }

    public void setCollector(Collector<Tuple2<K, Message>> collector) {
        this.collector = collector;
    }

    /**
     * This method is called before every superstep
     */
    public void preSuperstep() {
    }

    /**
     * This method is called after every superstep
     */
    public void postSuperStep() {
    }

    /**
     * Call this method to send a message to a vertex
     * that belongs to this or another partition.
     * The message will be delivered to the vertex during the same superstep. 
     * 
     * @param vertex The destination vertex ID
     * @param message The message content
     */
    protected void sendMessage(K vertex, Message message) {
        collector.collect(new Tuple2<>(vertex, message));
    }

    /**
     * This method is invoked once per partition in the beginning of each superstep.
     * It receives the current state of the vertices inside this partition and the
     * state of all out-going edges for each internal vertex. 
     * The method can generate messages to vertices inside this partition or other
     * partitions. The messages will be delivered during the same superstep.
     * 
     * @param edges the {@link RichEdge}s in this partition.
     * The source vertex of each RichEdge is an internal vertex
     * in this partition.
     * @throws Exception
     */
    public abstract void processPartition(Iterable<RichEdge<K, VV, EV>> edges) throws Exception;
}
