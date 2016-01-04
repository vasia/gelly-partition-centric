/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.graph.partition.centric;
import java.util.Random;


public class ConnectedComponentsData {

    public static final String getEnumeratingVertices(int num) {
        if (num < 1 || num > 1000000) {
            throw new IllegalArgumentException();
        }

        StringBuilder bld = new StringBuilder(3 * num);
        for (int i = 1; i <= num; i++) {
            bld.append(i);
            bld.append('\n');
        }
        return bld.toString();
    }

    /**
     * Creates random edges such that even numbered vertices are connected with even numbered vertices
     * and odd numbered vertices only with other odd numbered ones.
     */
    public static final String getRandomOddEvenEdges(int numEdges, int numVertices, long seed) {
        if (numVertices < 2 || numVertices > 1000000 || numEdges < numVertices || numEdges > 1000000) {
            throw new IllegalArgumentException();
        }

        StringBuilder bld = new StringBuilder(5 * numEdges);

        // first create the linear edge sequence even -> even and odd -> odd to make sure they are
        // all in the same component
        for (int i = 3; i <= numVertices; i++) {
            bld.append(i - 2).append(' ').append(i).append('\n');
        }

        numEdges -= numVertices - 2;
        Random r = new Random(seed);

        for (int i = 1; i <= numEdges; i++) {
            int evenOdd = r.nextBoolean() ? 1 : 0;

            int source = r.nextInt(numVertices) + 1;
            if (source % 2 != evenOdd) {
                source--;
                if (source < 1) {
                    source = 2;
                }
            }

            int target = r.nextInt(numVertices) + 1;
            if (target % 2 != evenOdd) {
                target--;
                if (target < 1) {
                    target = 2;
                }
            }

            bld.append(source).append(' ').append(target).append('\n');
        }
        return bld.toString();
    }

    private ConnectedComponentsData() {}
}
