/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.expression.accessor;

import io.streamthoughts.kafka.connect.filepulse.expression.EvaluationContext;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class PropertyAccessors {

    private final EvaluationContext context;

    public PropertyAccessors(final EvaluationContext context) {
        this.context = Objects.requireNonNull(context, "'context' should not be null");
    }

    public void writeValueForProperty(final Object target,
                                      final String name,
                                      final Object newValue) throws AccessException {

        List<PropertyAccessor> specificAccessors = findSpecificAccessorsToWrite(target, name);
        if (!specificAccessors.isEmpty() && evaluateWriters(target, name, newValue, specificAccessors)) {
            return;
        }

        List<PropertyAccessor> genericAccessors = findGenericAccessorsToWrite(target, name);
        if (!genericAccessors.isEmpty() && evaluateWriters(target, name, newValue, genericAccessors)) {
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

    public Object readPropertyValue(final Object target,
                                    final String name) throws AccessException {

        List<PropertyAccessor> specificAccessors = findSpecificAccessorsToRead(target, name);
        if (!specificAccessors.isEmpty()) {
            Object value = evaluateReaders(target, name, specificAccessors);
            if (value != null) return value;
        }

        List<PropertyAccessor> genericAccessors = findGenericAccessorsToRead(target, name);
        if (!genericAccessors.isEmpty()) {
            return evaluateReaders(target, name, genericAccessors);
        }

        throw new AccessException(
                String.format(
                        "Cannot found any property accessor for type '%s' and property %s",
                        target.getClass().getCanonicalName(),
                        name
                )
        );
    }

    private Boolean evaluateWriters(final Object target,
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

    private Object evaluateReaders(final Object target,
                                   final String name,
                                   final List<PropertyAccessor> specifics) {
        Iterator<PropertyAccessor> it = specifics.iterator();
        Object value = null;
        while (it.hasNext() && value == null) {
            PropertyAccessor accessor = it.next();
            value = accessor.read(context, target, name);
        }
        return value;
    }

    /**
     * Helpers methods to find generic write accessors for the given arguments.
     *
     * @param target  the target object.
     * @param name    the field name.
     * @return a list of {@link PropertyAccessor} candidates.
     */
    public List<PropertyAccessor> findGenericAccessorsToWrite(final Object target,
                                                              final String name) {
        return context.getPropertyAccessors()
                .stream()
                .filter(accessor -> !isSpecificAccessor(accessor))
                .filter(accessor -> accessor.canWrite(context, target, name))
                .collect(Collectors.toList());
    }

    /**
     * Helpers methods to find specific write accessors for the given arguments.
     *
     * @param target the target object.
     * @param name   the field name.
     * @return a list of {@link PropertyAccessor} candidates.
     */
    public List<PropertyAccessor> findSpecificAccessorsToWrite(final Object target,
                                                               final String name) {
        Class<?> type = target instanceof Class ? (Class<?>) target : target.getClass();
        return context.getPropertyAccessors()
                .stream()
                .filter(accessor -> isAccessorSpecificForType(type, accessor))
                .filter(accessor -> accessor.canWrite(context, target, name))
                .collect(Collectors.toList());
    }

    /**
     * Helpers methods to find generic read accessors for the given arguments.
     *
     * @param target the target object.
     * @param name   the field name.
     * @return a list of {@link PropertyAccessor} candidates.
     */
    public List<PropertyAccessor> findGenericAccessorsToRead(final Object target,
                                                             final String name) {
        return context.getPropertyAccessors()
                .stream()
                .filter(accessor -> !isSpecificAccessor(accessor))
                .filter(accessor -> accessor.canRead(context, target, name))
                .collect(Collectors.toList());
    }

    /**
     * Helpers methods to find specific read accessors for the given arguments.
     *
     * @param target the target object.
     * @param name   the field name.
     * @return a list of {@link PropertyAccessor} candidates.
     */
    public List<PropertyAccessor> findSpecificAccessorsToRead(final Object target,
                                                              final String name) {
        Class<?> type = target instanceof Class ? (Class<?>) target : target.getClass();
        return context.getPropertyAccessors()
                .stream()
                .filter(accessor -> isAccessorSpecificForType(type, accessor))
                .filter(accessor -> accessor.canRead(context, target, name))
                .collect(Collectors.toList());
    }

    private static boolean isAccessorSpecificForType(final Class<?> type, PropertyAccessor accessor) {
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

    private static boolean isSpecificAccessor(final PropertyAccessor accessor) {
        return accessor.getSpecificTargetClasses() != null && accessor.getSpecificTargetClasses().length > 0;
    }
}
