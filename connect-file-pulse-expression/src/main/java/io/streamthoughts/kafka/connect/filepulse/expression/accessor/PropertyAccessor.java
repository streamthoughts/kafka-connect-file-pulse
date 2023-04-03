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
