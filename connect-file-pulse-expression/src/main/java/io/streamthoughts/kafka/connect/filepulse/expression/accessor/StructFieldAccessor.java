/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.expression.accessor;

import io.streamthoughts.kafka.connect.filepulse.expression.EvaluationContext;
import java.util.Objects;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;

public class StructFieldAccessor implements PropertyAccessor {

    /**
     * {@inheritDoc}
     */
    @Override
    public Class<?>[] getSpecificTargetClasses() {
        return new Class[]{Struct.class, SchemaAndValue.class};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean canRead(final EvaluationContext context,
                           final Object target,
                           final String name) throws AccessException {

        return Schema.Type.STRUCT.equals(((SchemaAndValue)target).schema().type());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SchemaAndValue read(final EvaluationContext context,
                               final Object target,
                               final String name) throws AccessException {
        Objects.requireNonNull(target, "target cannot be null");
        Objects.requireNonNull(name, "name cannot be null");

        final Struct struct = getStructValue(target);

        Field field = struct.schema().field(name);
        if (field != null) {
            return new SchemaAndValue(field.schema(), struct.get(field));
        } else if (isDotPropertyAccessPath(name)) {
            String[] split = name.split("\\.", 2);
            Object rootObject = read(context, target, split[0]);
            if (rootObject != null) {
                return read(context, rootObject, split[1]);
            }
        }

        throw new AccessException("Can't access field '" + name + "' from Struct - field does not exist");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void write(final EvaluationContext context,
                      final Object target,
                      final String name,
                      final Object newValue) throws AccessException {
        throw new UnsupportedOperationException("Cannot write to new field into Struct/SchemaAndValue object");
    }

    private Struct getStructValue(final Object target) {
        if (target instanceof SchemaAndValue) {
            return (Struct) ((SchemaAndValue)target).value();
        }
        return (Struct) target;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean canWrite(final EvaluationContext context,
                            final Object target,
                            final String name) throws AccessException {
        return false;
    }

    private boolean isDotPropertyAccessPath(final String name) {
        return name.contains(".");
    }
}
