/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.expression.accessor;

import io.streamthoughts.kafka.connect.filepulse.expression.EvaluationContext;

public interface PropertyAccessor {

    /**
     * Returns {@code null} which means this is a general purpose accessor.
     *
     * @return the array of {@link Class} supported type.
     */
    default Class<?>[] getSpecificTargetClasses() {
        return null;
    }

    /**
     * Checks whether this accessor can read the specified variable name.
     *
     * @param context   the {@link EvaluationContext} instance.
     * @param target    the target object.
     * @param name      the variable name.
     *
     * @return  {@code true} if is readable.
     *
     * @throws AccessException if an error occurred while evaluating the variable.
     */
    boolean canRead(final EvaluationContext context,
                    final Object target,
                    final String name) throws AccessException;

    /**
     * Reads the specified variable from the target object.
     *
     * @param context   the {@link EvaluationContext} instance.
     * @param target    the target object.
     * @param name      the variable name to read.
     *
     * @return the value.
     *
     * @throws AccessException if an error occurred while evaluating the variable.
     */
    Object read(final EvaluationContext context,
                final Object target,
                final String name) throws AccessException;

    /**
     * Writes a new value into the specified target object.
     *
     * @param context   the {@link EvaluationContext} instance.
     * @param target    the target object.
     * @param name      the variable name.
     * @param newValue  the new value to write.
     *
     * @throws AccessException if an error occurred while evaluating the variable.
     */
    void write(final EvaluationContext context,
               final Object target,
               final String name,
               final Object newValue) throws AccessException;

    /**
     * Checks whether this accessor can write the newValue to the specified target object.
     *
     * @param context   the {@link EvaluationContext} instance.
     * @param target    the target object.
     * @param name      the variable name.
     *
     * @return  {@code true} if is writable.
     *
     * @throws AccessException if an error occurred while evaluating the variable.
     */
    boolean canWrite(final EvaluationContext context,
                     final Object target,
                     final String name) throws AccessException;
}
