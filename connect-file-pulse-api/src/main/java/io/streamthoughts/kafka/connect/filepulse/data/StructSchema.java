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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StructSchema implements Schema, Iterable<TypedField> {

    private static final Logger LOG = LoggerFactory.getLogger(StructSchema.class);

    private final Map<String, TypedField> fields;

    private Integer hash;

    private String name;

    private String namespace;

    private String doc;

    /**
     * Creates a new {@link StructSchema} instance.
     */
    public StructSchema() {
        this(Collections.emptyList(), null);
    }

    /**
     * Creates a new {@link StructSchema} instance.
     *
     * @param schema the {@link StructSchema} instance.
     */
    public StructSchema(final StructSchema schema) {
        this(schema.fields(), schema.name);
        this.namespace = schema.namespace;
        this.doc = schema.doc;
    }

    /**
     * Creates a new {@link StructSchema} instance.
     *
     * @param fields the collection {@link TypedField} instances.
     * @param name   the name of the schema.
     */
    public StructSchema(final Collection<TypedField> fields, final String name) {
        this.fields = new LinkedHashMap<>();
        this.name = name;
        fields.forEach(field -> this.fields.put(field.name(), field));
    }

    public StructSchema field(final String fieldName, final Schema fieldSchema) {
        if (fieldName == null || fieldName.isEmpty()) {
            throw new DataException("fieldName cannot be null.");
        }
        if (null == fieldSchema) {
            throw new DataException("fieldSchema for field " + fieldName + " cannot be null.");
        }
        if (fields.containsKey(fieldName)) {
            throw new DataException("Cannot create field because of field name duplication " + fieldName);
        }
        fields.put(fieldName, new TypedField(fields.size(), fieldSchema, fieldName));
        return this;
    }

    int indexOf(final String fieldName) {
        if (fieldName == null || fieldName.isEmpty()) {
            throw new DataException("fieldName cannot be null.");
        }
        return fields.containsKey(fieldName) ? fields.get(fieldName).index() : -1;
    }

    public TypedField field(final String fieldName) {
        if (fieldName == null || fieldName.isEmpty()) {
            throw new DataException("fieldName cannot be null.");
        }
        return fields.get(fieldName);
    }

    public List<TypedField> fields() {
        ArrayList<TypedField> ordered = new ArrayList<>(fields.values());
        ordered.sort(Comparator.comparing(TypedField::name));
        return ordered;
    }

    public List<TypedField> fieldsByIndex() {
        ArrayList<TypedField> ordered = new ArrayList<>(fields.values());
        // order elements in array to match field column index
        for (TypedField field: fields.values()) {
            ordered.add(field.index(),field);
        }
        return ordered;
    }

    void set(final String fieldName, final Schema fieldSchema) {
        if (fieldName == null || fieldName.isEmpty()) {
            throw new DataException("fieldName cannot be null.");
        }
        if (null == fieldSchema) {
            throw new DataException("fieldSchema for field " + fieldName + " cannot be null.");
        }
        TypedField field = field(fieldName);
        if (field == null) {
            throw new DataException("Cannot set field because of field do not exist " + fieldName);
        }
        fields.put(fieldName, new TypedField(field.index(), fieldSchema, fieldName));
    }

    void rename(final String fieldName, final String newField) {
        TypedField tf = fields.remove(fieldName);
        if (tf == null) {
            throw new DataException("Cannot rename field because of field do not exist " + fieldName);
        }
        fields.put(newField, new TypedField(tf.index(), tf.schema(), newField));
    }

    TypedField remove(final String fieldName) {
        final TypedField tf = field(fieldName);
        if (tf == null) return null;

        fields.remove(tf.name());
        fields.replaceAll((k, v) -> {
            if (v.index() > tf.index()) {
                return new TypedField(v.index() - 1, v.schema(), v.name());
            }
            return v;
        });
        return tf;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<TypedField> iterator() {
        return fields().iterator();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Type type() {
        return Type.STRUCT;
    }

    /**
     * Gets the name for this schema.
     *
     * @return the schema name.
     */
    public String name() {
        return this.name;
    }

    /**
     * Sets the name for this schema.
     *
     * @param name the schema name.
     * @return {@code this}
     */
    public StructSchema name(final String name) {
        this.name = name;
        return this;
    }

    /**
     * Gets the namespace for this schema.
     *
     * @return the schema namespace.
     */
    public String namespace() {
        return namespace;
    }

    /**
     * Sets the namespace for this schema.
     *
     * @param namespace the namespace.
     * @return {@code this}
     */
    public StructSchema namespace(final String namespace) {
        this.namespace = namespace;
        return this;
    }

    /**
     * Gets the doc for this schema.
     *
     * @return the doc.
     */
    public String doc() {
        return this.doc;
    }

    /**
     * Sets the doc for this schema.
     *
     * @param doc the schema doc.
     * @return {@code this}
     */
    public StructSchema doc(final String doc) {
        this.doc = doc;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T map(SchemaMapper<T> mapper, boolean optional) {
        return mapper.map(this, optional);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T map(final SchemaMapperWithValue<T> mapper, final Object object, final boolean optional) {
        return mapper.map(this, (TypedStruct) object, optional);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Schema merge(final Schema o) {
        if (this.equals(o)) return this;

        if (!(o instanceof StructSchema)) {
            // StructSchema cannot only be merged with another StructSchema,
            // so let's try to reverse the merge (e.g. ARRAY can be merged with a STRUCT).
            return o.merge(this);
        }

        final StructSchema that = (StructSchema) o;

        return new StructSchemaMerger().apply(this, that);

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof StructSchema)) return false;
        StructSchema that = (StructSchema) o;
        return Objects.equals(fields, that.fields);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        if (hash == null) {
            hash = Objects.hash(fields);
        }
        return hash;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "[" +
                "fields=" + fields +
                ']';
    }

    static class StructSchemaMerger implements BiFunction<StructSchema, StructSchema, StructSchema> {

        /**
         * {@inheritDoc}
         *
         * @return  a new {@link StructSchema} resulting from the merge of the given two {@link StructSchema}.
         */
        @Override
        public StructSchema apply(final StructSchema left, final StructSchema right) {

            if (!Objects.equals(left.name, right.name))
                throw new DataException(
                        "Cannot merge two schemas wih different name " + left.name() + "<>" + right.name());

            if (!Objects.equals(left.namespace, right.namespace))
                throw new DataException(
                        "Cannot merge two schemas wih different namespace " + left.name() + "<>" + right.name());

            LOG.debug("Merging schemas with namespace: {}, name: {}", left.name, left.namespace);
            final StructSchema merged = new StructSchema()
                    .name(left.name)
                    .namespace(left.namespace)
                    .doc(left.doc);

            final HashMap<String, TypedField> remaining = new HashMap<>(left.fields);

            // Iterator on RIGHT fields and compare to LEFT fields.
            for (final TypedField rightField : right.fields()) {

                final String name = rightField.name();
                LOG.debug("Merging field: name={}, ", name);

                // field exist only on RIGHT schema.
                if (!remaining.containsKey(name)) {
                    merged.field(name, rightField.schema());
                    continue;
                }

                // field exist on both LEFT and RIGHT schemas.
                final TypedField leftField = remaining.remove(name);

                try {
                    final Schema fieldMergedSchema = leftField.schema().merge(rightField.schema());
                    merged.field(name, fieldMergedSchema);
                } catch (Exception e) {
                    throw new DataException("Failed to merge schemas for field '" + name + "'. ", e);
                }
            }

            // remaining fields that existing only on LEFT schema.
            remaining.values().forEach(it -> merged.field(it.name(), it.schema()));

            return merged;
        }
    }
}
