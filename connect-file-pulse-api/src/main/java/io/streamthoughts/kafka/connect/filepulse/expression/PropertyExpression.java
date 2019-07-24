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

import io.streamthoughts.kafka.connect.filepulse.expression.accessor.AccessException;
import io.streamthoughts.kafka.connect.filepulse.expression.accessor.PropertyAccessor;
import io.streamthoughts.kafka.connect.filepulse.expression.converter.Converters;
import io.streamthoughts.kafka.connect.filepulse.expression.converter.PropertyConverter;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class PropertyExpression implements Expression {

    private final String original;
    private final String rootObject;
    private final String attribute;

    /**
     * Creates a new {@link PropertyExpression} instance.
     *
     * @param original      the original expression.
     * @param rootObject    the root object.
     * @param attribute     the attribute path to access.
     */
    public PropertyExpression(final String original,
                              final String rootObject,
                              final String attribute) {
        Objects.requireNonNull(original, "original cannot be null");
        Objects.requireNonNull(rootObject, "rootObject cannot be null");
        this.original = original;
        this.rootObject = rootObject;
        this.attribute = attribute;
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public Object readValue(final EvaluationContext context) {
        return readValue(context, Object.class);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T> T readValue(final EvaluationContext context, final Class<T> expectedType) {
        Objects.requireNonNull(context, " context cannot be null");
        Objects.requireNonNull(expectedType, " expectedType cannot be null");

        Object returned = getValueForProperty(context, context.rootObject(), rootObject);

        if (attribute != null) {
            if (returned == null) {
                throw new ExpressionException(
                    "Cannot evaluate attribute expression '" + attribute + "', root object'"
                    + rootObject + "' returned null."
                );
            }
            returned = getValueForProperty(context, returned, attribute);
        }

        if (returned != null && expectedType.isAssignableFrom(returned.getClass())) {
            return (T)returned;
        }

        final List<PropertyConverter> converters = context.getPropertyConverter();
        return Converters.converts(converters, returned, expectedType);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeValue(final Object value, final EvaluationContext context) {
        Object target = context.rootObject();

        if (attribute == null) {
            setValueForProperty(context, target, rootObject, value);
        } else {
            target = getValueForProperty(context, target, rootObject);
            setValueForProperty(context, target, attribute, value);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String originalExpression() {
        return original;
    }

    private void setValueForProperty(final EvaluationContext context,
                                     final Object target,
                                     final String name,
                                     final Object newValue) {

        List<PropertyAccessor> specificAccessors = findSpecificAccessorsToWrite(context, target, name);
        if (!specificAccessors.isEmpty() && evaluateWriters(context, target, name, newValue, specificAccessors)) {
            return;
        }

        List<PropertyAccessor> genericAccessors = findGenericAccessorsToWrite(context, target, name);
        if (!genericAccessors.isEmpty() && evaluateWriters(context, target, name, newValue, genericAccessors)) {
            return;
        }

        if (specificAccessors.isEmpty() && genericAccessors.isEmpty()) {
            throw new AccessException(String.format(
                "Can't found any property accessor for type '%s' and property %s",
                target.getClass().getCanonicalName(),
                name)
            );
        }
    }

    private Object getValueForProperty(final EvaluationContext context,
                                       final Object target,
                                       final String name) {

        List<PropertyAccessor> specificAccessors = findSpecificAccessorsToRead(context, target, name);
        if (!specificAccessors.isEmpty()) {
            Object value = evaluateReaders(context, target, name, specificAccessors);
            if (value != null) return value;
        }

        List<PropertyAccessor> genericAccessors = findGenericAccessorsToRead(context, target, name);
        if (!genericAccessors.isEmpty()) {
            return evaluateReaders(context, target, name, genericAccessors);
        }

        throw new AccessException(
            String.format(
                "Can't found any property accessor for type '%s' and property %s",
                target.getClass().getCanonicalName(),
                name)
        );
    }

    private Boolean evaluateWriters(final EvaluationContext context,
                                    final Object target,
                                    final String name,
                                    final Object newValue,
                                    final List<PropertyAccessor> specifics) {
        Iterator<PropertyAccessor> it = specifics.iterator();
        boolean run = false;
        while (it.hasNext() && !run) {
            PropertyAccessor accessor = it.next();
            accessor.write(context, target, name, newValue);
            run = true;
        }
        return run;
    }

    private Object evaluateReaders(final EvaluationContext context,
                                   final Object target,
                                   final String name,
                                   final List<PropertyAccessor> specifics) {
        Iterator<PropertyAccessor> it = specifics.iterator();
        Object value = null;
        while (it.hasNext() && value == null ) {
            PropertyAccessor accessor = it.next();
            value = accessor.read(context, target, name);
        }
        return value;
    }

    private List<PropertyAccessor> findGenericAccessorsToWrite(final EvaluationContext context,
                                                               final Object target,
                                                               final String name) {
        return context.getPropertyAccessors()
            .stream()
            .filter(accessor -> !isSpecificAccessor(accessor))
            .filter(accessor -> accessor.canWrite(context, target, name))
            .collect(Collectors.toList());
    }

    private List<PropertyAccessor> findSpecificAccessorsToWrite(final EvaluationContext context,
                                                                final Object target,
                                                                final String name) {
        Class<?> type = target instanceof Class ? (Class<?>) target : target.getClass();
        return context.getPropertyAccessors()
            .stream()
            .filter(accessor -> isAccessorSpecificForType(type, accessor))
            .filter(accessor -> accessor.canWrite(context, target, name))
            .collect(Collectors.toList());
    }

    private List<PropertyAccessor> findGenericAccessorsToRead(final EvaluationContext context,
                                                              final Object target,
                                                              final String name) {
        return context.getPropertyAccessors()
            .stream()
            .filter(accessor -> !isSpecificAccessor(accessor))
            .filter(accessor -> accessor.canRead(context, target, name))
            .collect(Collectors.toList());
    }

    private List<PropertyAccessor> findSpecificAccessorsToRead(final EvaluationContext context,
                                                               final Object target,
                                                               final String name) {
        Class<?> type = target instanceof Class ? (Class<?>) target : target.getClass();
        return context.getPropertyAccessors()
            .stream()
            .filter(accessor -> isAccessorSpecificForType(type, accessor))
            .filter(accessor -> accessor.canRead(context, target, name))
            .collect(Collectors.toList());
    }

    private boolean isAccessorSpecificForType(final Class<?> type, PropertyAccessor accessor) {
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
        if (!(o instanceof PropertyExpression)) return false;
        PropertyExpression that = (PropertyExpression) o;
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
                ']';
    }
}
