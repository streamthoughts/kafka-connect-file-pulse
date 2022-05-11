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
package io.streamthoughts.kafka.connect.filepulse.data;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

public class LazyArraySchema extends ArraySchema implements Schema {

    private final Collection<?>  list;

    private Schema valueSchema;

    /**
     * Creates a new {@link LazyArraySchema} for the specified type.
     *
     * @param list the {@link List} instance.
     */
    LazyArraySchema(final Collection<?> list) {
        super(null);
        this.list = list;

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Schema valueSchema() {
        if (valueSchema == null) {
            if (list.isEmpty()) {
                throw new DataException("Cannot infer value type because LIST is empty");
            }

            final Iterator<?> iterator = list.iterator();
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
        return valueSchema != null || !list.isEmpty();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        LazyArraySchema that = (LazyArraySchema) o;
        return Objects.equals(list, that.list) && Objects.equals(valueSchema, that.valueSchema);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return Objects.hash(type(), valueSchema);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "LazyArraySchema{" +
                "list=" + list +
                ", valueSchema=" + valueSchema +
                '}';
    }
}