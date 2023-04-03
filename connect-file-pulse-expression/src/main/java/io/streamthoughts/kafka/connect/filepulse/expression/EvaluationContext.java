/*
 * Copyright 2019-2020 StreamThoughts.
 *
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

import io.streamthoughts.kafka.connect.filepulse.expression.accessor.PropertyAccessor;
import io.streamthoughts.kafka.connect.filepulse.expression.converter.PropertyConverter;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface EvaluationContext {

    /**
     * Return the default root context object against which an expression is evaluated.
     *
     * @return the root {@link Object}.
     */
    Object rootObject();

    /**
     * Return a list of accessors that will be asked in turn to read/write a property.
     *
     * @return the list of {@link PropertyAccessor} instance.
     */
    List<PropertyAccessor> getPropertyAccessors();

    /**
     * Return a list of converter that will be asked in turn to convert read value into expected type.
     *
     * @return the list of {@link PropertyConverter} instance.
     */
    List<PropertyConverter> getPropertyConverter();

    /**
     * Checks whether a variable is defined in this context.
     *
     * @param name  the variable name.
     *
     * @return {@code true} if the variable exists for the given name.
     */
    boolean has(final String name);

    /**
     * Returns the variable value for the specified name.
     *
     * @param name  the variable name.
     *
     * @return the variable value or {@code null} of no variable exist for the given name.
     */
    Object get(final String name);

    /**
     * Sets a variable in this context with the specified name and value.
     *
     * @param name  the variable name.
     * @param value the variable value.
     */
    void set(final String name, final Object value);

    /**
     * Returns the list of variables defined in this context.
     *
     * @return  a set of variable name.
     */
    Set<String> variables();

    /**
     * Returns the values of variables defined in this context.
     * @return  a map of variables.
     */
    Map<String, Object> values();
}
