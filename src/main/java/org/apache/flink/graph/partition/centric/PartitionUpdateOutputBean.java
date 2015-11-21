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

import java.io.Serializable;

/**
 * This class is used to store a result of a computation step on a graph partition.
 */
public class PartitionUpdateOutputBean<K, VV, Message> implements Serializable {
    private static final long serialVersionUID = 1L;

    private boolean isVertex;
    private K id;
    private VV value;
    private Message messageContent;

    public PartitionUpdateOutputBean() {
    }

    public boolean isVertex() {
        return isVertex;
    }

    public void setVertex(boolean vertex) {
        this.isVertex = vertex;
    }

    public K getId() {
        return id;
    }

    public void setId(K id) {
        this.id = id;
    }

    public VV getValue() {
        return value;
    }

    public void setValue(VV value) {
        this.value = value;
    }

    public void setMessageContent(Message messageContent) {
        this.messageContent = messageContent;
    }

    public Message getMessageContent() {
        return messageContent;
    }

    public Vertex<K, VV> getVertex() {
        return new Vertex<>(id, value);
    }

    public void setVertex(Vertex<K, VV> vertex) {
        this.id = vertex.getId();
        this.value = vertex.getValue();
        this.isVertex = true;
    }

    public void setMessage(Tuple2<K, Message> message) {
        this.id = message.f0;
        this.messageContent = message.f1;
        this.isVertex = false;
    }

    public Tuple2<K, Message> getMessage() {
        return new Tuple2<>(id, messageContent);
    }
}
