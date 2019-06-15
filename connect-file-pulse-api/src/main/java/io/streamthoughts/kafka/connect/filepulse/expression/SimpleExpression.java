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

import io.streamthoughts.kafka.connect.filepulse.expression.accessor.PropertyAccessor;
import io.streamthoughts.kafka.connect.filepulse.expression.accessor.StructFieldAccessor;
import io.streamthoughts.kafka.connect.filepulse.expression.function.ExpressionFunctionExecutor;
import org.apache.kafka.connect.data.SchemaAndValue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class SimpleExpression implements Expression {

    private final String original;
    private final String rootObject;
    private final String attribute;
    private final ExpressionFunctionExecutor function;

    private final List<PropertyAccessor> accessors = new ArrayList<>();

    /**
     * Creates a new {@link SimpleExpression} instance.
     * @param original      the original expression.
     * @param rootObject    the root object.
     * @param attribute     the attribute path to access.
     */
    public SimpleExpression(final String original,
                            final String rootObject,
                            final String attribute) {
        this(original, rootObject, attribute, null);
    }


    /**
     * Creates a new {@link SimpleExpression} instance.
     * @param original      the original expression.
     * @param rootObject    the root object.
     * @param attribute     the attribute path to access.
     * @param function      the function to be apply on the acceded field.
     */
    public SimpleExpression(final String original,
                            final String rootObject,
                            final String attribute,
                            final ExpressionFunctionExecutor function) {
        Objects.requireNonNull(original, "original cannot be null");
        Objects.requireNonNull(rootObject, "rootObject cannot be null");
        this.original = original;
        this.rootObject = rootObject;
        this.attribute = attribute;
        this.function = function;

        register(new StructFieldAccessor());
    }

    private void register(final PropertyAccessor accessor) {
        accessors.add(accessor);
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public SchemaAndValue evaluate(final EvaluationContext context) {
        if (!context.has(rootObject)) {
            throw new ExpressionException(
                "Cannot access to variable '" + rootObject + "' from context : " + context.variables());
        }

        SchemaAndValue rootValue = context.get(rootObject);

        if (attribute  == null) {
            return mayExecuteFunctionAndGet(rootValue);
        }

        final SchemaAndValue property = getValueForProperty(context, rootValue.value(), attribute);
        return mayExecuteFunctionAndGet(property);
    }

    private SchemaAndValue mayExecuteFunctionAndGet(final SchemaAndValue value) {
        if (function != null) {
            return function.execute(value);
        }
        return value;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String originalExpression() {
        return original;
    }

    private SchemaAndValue getValueForProperty(final EvaluationContext context,
                                               final Object target,
                                               final String name) {

        List<PropertyAccessor> specificAccessors = findSpecificAccessorsToRead(context, target, name);
        if (!specificAccessors.isEmpty()) {
            SchemaAndValue value = evaluateReaders(context, target, name, specificAccessors);
            if (value != null) return value;
        }

        List<PropertyAccessor> genericAccessors = findGenericAccessorsToRead(context, target, name);
        if (!genericAccessors.isEmpty()) {
            return evaluateReaders(context, target, name, genericAccessors);
        }

        throw new RuntimeException(
            String.format(
                "Can't found any property accessor for type '%s' and property %s",
                target.getClass().getCanonicalName(),
                name)
        );
    }

    private SchemaAndValue evaluateReaders(final EvaluationContext context,
                                      final Object target,
                                      final String name,
                                      final List<PropertyAccessor> specifics) {
        Iterator<PropertyAccessor> it = specifics.iterator();
        SchemaAndValue value = null;
        while (it.hasNext() && value == null ) {
            PropertyAccessor accessor = it.next();
            value = accessor.read(context, target, name);
        }
        return value;
    }

    private List<PropertyAccessor> findGenericAccessorsToRead(final EvaluationContext context,
                                                              final Object target,
                                                              final String name) {
        return accessors.stream()
            .filter(accessor -> !isSpecificAccessor(accessor))
            .filter(accessor -> accessor.canRead(context, target, name))
            .collect(Collectors.toList());
    }

    private List<PropertyAccessor> findSpecificAccessorsToRead(final EvaluationContext context,
                                                               final Object target,
                                                               final String name) {
        Class<?> type = target instanceof Class ? (Class<?>) target : target.getClass();
    return accessors.stream()
            .filter(accessor -> isAccessorSpecificForType(type, accessor))
            .filter(accessor -> accessor.canRead(context, target, name))
            .collect(Collectors.toList());
    }

    private boolean isAccessorSpecificForType(Class<?> type, PropertyAccessor accessor) {
        if (isSpecificAccessor(accessor)) {
            Class<?>[] specificTargetClasses = accessor.getSpecificTargetClasses();
            List<Class<?>> l = Arrays
                .stream(specificTargetClasses)
                .filter(clazz -> clazz.isAssignableFrom(type))
                .collect(Collectors.toList());
            return l.size() > 0;
        }
        return false;
    }

    private boolean isSpecificAccessor(final PropertyAccessor accessor) {
        return accessor.getSpecificTargetClasses() != null && accessor.getSpecificTargetClasses().length > 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SimpleExpression)) return false;
        SimpleExpression that = (SimpleExpression) o;
        return Objects.equals(original, that.original) &&
                Objects.equals(rootObject, that.rootObject) &&
                Objects.equals(attribute, that.attribute);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return Objects.hash(rootObject, attribute);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "[" +
                "original=" + original +
                ", rootObject=" + rootObject +
                ", attribute=" + attribute +
                ", function=" + function +
                ']';
    }
}
