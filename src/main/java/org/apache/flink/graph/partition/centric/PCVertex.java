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

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;

import java.util.HashMap;

/**
 * Represents a partition centric graph's nodes.
 * It carries the vertex id, vertex value and all outgoing edges
 * For vertices or edges with no value, use {@link org.apache.flink.types.NullValue} as the value type.
 *
 * @param <K> The type of a vertex's id
 * @param <VV> The type of a vertex's value
 * @param <EV> The type of an edge's value
 */
public class PCVertex<K, VV, EV> extends Tuple3<K, VV, Boolean> {
    private static final long serialVersionUID = 1L;

    public PCVertex() {
        this.f2 = false;
    }

    public PCVertex(K id, VV value, Boolean updated) {
        this.f0 = id;
        this.f1 = value;
        this.f2 = updated;
    }

    public K getId() {
        return f0;
    }

    public void setId(K id) {
        this.f0 = id;
    }

    public VV getValue() {
        return f1;
    }

    public void setValue(VV value) {
        this.f1 = value;
    }

    public boolean isUpdated() {
        return f2;
    }

    public void setUpdated(boolean updated) {
        this.f2 = updated;
    }

}
