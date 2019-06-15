/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.streamthoughts.kafka.connect.filepulse.expression;

import org.apache.kafka.connect.data.SchemaAndValue;

import java.util.Map;
import java.util.Set;

public interface EvaluationContext {

    /**
     * Checks whether a variable is defined in this context.
     * @param name  the variable name.
     *
     * @return {@code true} if the variable exists for the given name.
     */
    boolean has(final String name);

    /**
     * Returns the variable value for the specified name.
     * @param name  the variable name.
     *
     * @return the variable value or {@code null} of no variable exist for the given name.
     */
    SchemaAndValue get(final String name);

    /**
     * Sets a variable in this context with the specified name and value.
     * @param name  the variable name.
     * @param value the variable value./
     */
    void set(final String name, final SchemaAndValue value);

    /**
     * Returns the list of variables defined in this context.
     * @return  a set of variable name.
     */
    Set<String> variables();

    Map<String, SchemaAndValue> values();
}
