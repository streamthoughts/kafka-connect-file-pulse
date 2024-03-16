/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.expression.accessor;

import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.data.TypedValue;
import io.streamthoughts.kafka.connect.filepulse.expression.EvaluationContext;
import java.util.Objects;

public class TypedStructAccessor implements PropertyAccessor {

    /**
     * {@inheritDoc}
     */
    @Override
    public Class<?>[] getSpecificTargetClasses() {
        return new Class[]{TypedStruct.class};
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public boolean canRead(final EvaluationContext context,
                           final Object target,
                           final String name) throws AccessException {
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TypedValue read(final EvaluationContext context,
                           final Object target,
                           final String name) throws AccessException {
        Objects.requireNonNull(target, "target cannot be null");
        Objects.requireNonNull(name, "name cannot be null");

        final TypedValue value = ((TypedStruct) target).find(name);
        if (value == null) {
            throw new AccessException("Cannot access to field '" + name + "'");
        }
        return value;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void write(final EvaluationContext context,
                      final Object target,
                      final String name,
                      final Object newValue) throws AccessException {

        Objects.requireNonNull(target, "target cannot be null");
        Objects.requireNonNull(name, "name cannot be null");
        ((TypedStruct)target).insert(name, newValue);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean canWrite(final EvaluationContext context,
                            final Object target,
                            final String name) throws AccessException {
        return true;
    }
}
