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
package io.streamthoughts.kafka.connect.filepulse.filter;

import io.streamthoughts.kafka.connect.filepulse.internal.SchemaUtils;
import io.streamthoughts.kafka.connect.filepulse.reader.RecordsIterable;
import io.streamthoughts.kafka.connect.filepulse.reader.FileInputRecord;
import io.streamthoughts.kafka.connect.filepulse.source.FileInputData;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static io.streamthoughts.kafka.connect.filepulse.internal.SchemaUtils.copySchemaBasics;
import static io.streamthoughts.kafka.connect.filepulse.internal.SchemaUtils.groupFieldByName;
import static io.streamthoughts.kafka.connect.filepulse.internal.SchemaUtils.isTypeOf;


public abstract class AbstractMergeRecordFilter<T extends AbstractRecordFilter> extends AbstractRecordFilter<T> {
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(final Map<String, ?> props) {
        super.configure(props);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RecordsIterable<FileInputData> apply(final FilterContext context,
                                                final FileInputData record,
                                                final boolean hasNext) throws FilterException {

        final RecordsIterable<FileInputData> filtered = apply(context, record);

        final List<FileInputData> merged = filtered
                .stream()
                .map(r -> merge(record, r, overwrite()))
                .collect(Collectors.toList());
        return new RecordsIterable<>(merged);
    }

    /**
     * Apply the filter logic for the specified data.
     *
     * @param record  a data.
     * @return  a new {@link FileInputRecord} instance.
     */
    abstract protected RecordsIterable<FileInputData> apply(final FilterContext context,
                                                            final FileInputData record) throws FilterException ;

    /**
     * Returns a list of fields that must be overwrite.
     *
     * @return a set of field names.
     */
    abstract protected Set<String> overwrite();

    /**
     * Straightforward method to merge two connect {@link Struct} instances.
     *
     * @param prevStruct
     * @param newStruct
     * @return a new {@link FileInputRecord} instance.
     */
    FileInputData merge(final FileInputData prevStruct,
                        final FileInputData newStruct,
                        final Set<String> overwrite) {

        final Schema leftSchema = prevStruct.schema();
        final Schema rightSchema = newStruct.schema();

        SchemaBuilder builder = copySchemaBasics(leftSchema);
        SchemaUtils.merge(leftSchema, rightSchema, builder, overwrite);

        Struct struct = new Struct(builder.build());
        copyValues(prevStruct.value(), struct);
        copyValues(newStruct.value(), struct);
        return new FileInputData(struct);
    }

    private static void copyValues(final Struct source, final Struct target) {
        Map<String, Field> sourceFieldByName = groupFieldByName(source.schema().fields());
        Map<String, Field> targetFieldByName = groupFieldByName(target.schema().fields());

        for (final Field fieldSource : sourceFieldByName.values()) {
            final String name = fieldSource.name();

            if (!targetFieldByName.containsKey(name)) {
                // should not happen.
                throw new FilterException("Cannot merge struct objects - no field '" + name + "' defined in target struct schema.");
            }
            Field fieldTarget = targetFieldByName.get(name);
            Object value = source.getWithoutDefault(name);
            if (value != null) {
                if (isTypeOf(fieldTarget, Schema.Type.ARRAY)) {
                    List<Object> objects = target.getArray(fieldSource.name());
                    if (objects == null) {
                        objects = new LinkedList<>();
                    }
                    if (isTypeOf(fieldSource, Schema.Type.ARRAY)) {
                        objects.addAll(source.getArray(name));
                    } else {
                        objects.add(value);
                    }
                    target.put(name, objects);
                } else {
                    // We simply copy the field value if target type is not an array
                    // should we check if source and target type are equal ???
                    target.put(name, value);
                }
            }
        }
    }
}
