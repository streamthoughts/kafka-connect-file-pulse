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

import io.streamthoughts.kafka.connect.filepulse.expression.accessor.HeadersAccessor;
import io.streamthoughts.kafka.connect.filepulse.expression.accessor.MapAdaptablePropertyAccessor;
import io.streamthoughts.kafka.connect.filepulse.expression.accessor.PropertyAccessor;
import io.streamthoughts.kafka.connect.filepulse.expression.accessor.ReflectivePropertyAccessor;
import io.streamthoughts.kafka.connect.filepulse.expression.accessor.StructFieldAccessor;
import io.streamthoughts.kafka.connect.filepulse.expression.accessor.TypedStructAccessor;
import io.streamthoughts.kafka.connect.filepulse.expression.converter.PrimitiveConverter;
import io.streamthoughts.kafka.connect.filepulse.expression.converter.PropertyConverter;
import io.streamthoughts.kafka.connect.filepulse.expression.converter.TypedValueConverter;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class StandardEvaluationContext implements EvaluationContext {

    private final Object rootObject;
    private final Map<String, Object> variables;

    private final List<PropertyAccessor> propertyAccessors;
    private final List<PropertyConverter> propertyConverters;

    /**
     * Creates a new {@link StandardEvaluationContext} instance.
     *
     * @param rootObject the root object.
     */
    public StandardEvaluationContext(final Object rootObject) {
        this(rootObject, new HashMap<>(), new LinkedList<>(), new LinkedList<>());
    }

    /**
     * Creates a new {@link StandardEvaluationContext} instance.
     *
     * @param rootObject the root object.
     * @param variables  the variables.
     */
    public StandardEvaluationContext(final Object rootObject,
                                     final Map<String, Object> variables) {
        this(rootObject, variables, new LinkedList<>(), new LinkedList<>());
    }

    /**
     * Creates a new {@link StandardEvaluationContext} instance.
     *
     * @param rootObject the root object.
     * @param variables  the variables.
     */
    private StandardEvaluationContext(final Object rootObject,
                                      final Map<String, Object> variables,
                                      final List<PropertyAccessor> propertyAccessors,
                                      final List<PropertyConverter> converters) {
        Objects.requireNonNull(rootObject, "rootObject cannot be null");
        Objects.requireNonNull(variables, "variables cannot be null");
        Objects.requireNonNull(propertyAccessors, "propertyAccessors cannot be null");
        this.rootObject = rootObject;
        this.variables = variables;
        this.propertyAccessors = propertyAccessors;
        this.propertyAccessors.add(new HeadersAccessor());
        this.propertyAccessors.add(new TypedStructAccessor());
        this.propertyAccessors.add(new StructFieldAccessor());
        this.propertyAccessors.add(new MapAdaptablePropertyAccessor());
        this.propertyAccessors.add(new ReflectivePropertyAccessor());

        this.propertyConverters = converters;
        this.propertyConverters.add(new PrimitiveConverter());
        this.propertyConverters.add(new TypedValueConverter());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object rootObject() {
        return rootObject;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<PropertyAccessor> getPropertyAccessors() {
        return propertyAccessors;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<PropertyConverter> getPropertyConverter() {
        return propertyConverters;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean has(final String name) {
        return variables.containsKey(name);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object get(final String name) {
        return variables.get(name);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void set(final String name, final Object value) {
        variables.put(name, value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<String> variables() {
        return variables.keySet();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, Object> values() {
        return variables;
    }
}
