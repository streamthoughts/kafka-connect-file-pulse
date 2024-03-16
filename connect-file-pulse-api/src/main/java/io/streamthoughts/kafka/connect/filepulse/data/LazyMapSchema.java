/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.data;

import java.util.Iterator;
import java.util.Map;

public class LazyMapSchema extends MapSchema implements Schema {

    private final Map map;

    private Schema valueSchema;

    /**
     * Creates a new LazyMapSchema for the specified type.
     */
    LazyMapSchema(final Map map) {
        super(null);
        this.map = map;

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Schema valueSchema() {
        if (valueSchema == null) {
            if (map.isEmpty()) {
                throw new DataException("Cannot infer value type because MAP is empty");
            }
            final Iterator<?> iterator = map.values().iterator();
            valueSchema = SchemaSupplier.lazy(iterator.next()).get();
            while (iterator.hasNext()) {
                valueSchema = valueSchema.merge(SchemaSupplier.lazy(iterator.next()).get());
            }
        }
        return valueSchema;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isResolvable() {
        return valueSchema!= null || !map.isEmpty();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object o) {
        return super.equals(o);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return super.hashCode();
    }
}