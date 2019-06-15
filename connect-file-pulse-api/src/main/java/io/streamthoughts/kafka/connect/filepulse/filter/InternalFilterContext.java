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

import io.streamthoughts.kafka.connect.filepulse.expression.SimpleEvaluationContext;
import io.streamthoughts.kafka.connect.filepulse.offset.SourceMetadata;
import io.streamthoughts.kafka.connect.filepulse.source.FileInputData;
import io.streamthoughts.kafka.connect.filepulse.source.FileInputOffset;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Simple {@link FilterContext} implementation class.
 */
public class InternalFilterContext extends SimpleEvaluationContext implements FilterContext {

    private static final String FIELD_FILE = "$file";
    private static final String FIELD_RECORD_OFFSET = "$offset";
    private static final String FIELD_RECORD_LINE = "$row";
    private static final String FIELD_RECORD_DATA = "$value";

    private static final String FIELD_FILE_NAME = "name";
    private static final String FIELD_FILE_HASH = "hash";
    private static final String FIELD_FILE_LAST_MODIFIED = "lastModified";
    private static final String FIELD_FILE_ABSOLUTE_PATH = "absPath";
    private static final String FIELD_FILE_PATH = "path";
    private static final String FIELD_FILE_SIZE = "size";

    private static final String EXCEPTION_FIELD = "$error";
    private static final String EXCEPTION_MESSAGE_FIELD = "message";
    private static final String EXCEPTION_FILTER_FIELD = "filter";

    private static final Schema EXCEPTION_SCHEMA = SchemaBuilder
            .struct()
            .field(EXCEPTION_MESSAGE_FIELD, Schema.STRING_SCHEMA).optional()
            .field(EXCEPTION_FILTER_FIELD, Schema.STRING_SCHEMA).optional()
            .build();

    private static final Schema FILE_SCHEMA = SchemaBuilder
            .struct()
            .field(FIELD_FILE_NAME, Schema.STRING_SCHEMA).optional()
            .field(FIELD_FILE_HASH, Schema.INT64_SCHEMA).optional()
            .field(FIELD_FILE_LAST_MODIFIED, Schema.INT64_SCHEMA).optional()
            .field(FIELD_FILE_ABSOLUTE_PATH, Schema.STRING_SCHEMA).optional()
            .field(FIELD_FILE_PATH, Schema.STRING_SCHEMA).optional()
            .field(FIELD_FILE_SIZE, Schema.INT64_SCHEMA).optional()
            .build();

    protected long size;

    private SourceMetadata metadata;

    private ExceptionContext exception;

    private FileInputOffset offset;


    /**
     * Creates a new {@link InternalFilterContext} for the specified arguments.
     *
     * @param metadata  the source metadata
     * @param offset    the record offset
     *
     * @return a new {@link InternalFilterContext} instance.
     */
    public static InternalFilterContext with(final SourceMetadata metadata,
                                             final FileInputOffset offset) {
        return new InternalFilterContext(metadata, offset, null, new HashMap<>(), null);
    }

    /**
     * Creates a new {@link InternalFilterContext} for the specified arguments.
     *
     * @param metadata  the source metadata
     * @param offset    the record offset
     * @param data      the record data
     *
     * @return a new {@link InternalFilterContext} instance.
     */
    public static InternalFilterContext with(final SourceMetadata metadata,
                                             final FileInputOffset offset,
                                             final FileInputData data) {
        return new InternalFilterContext(metadata, offset, data, new HashMap<>(), null);
    }

    /**
     * Creates a new {@link InternalFilterContext} for the specified arguments.
     *
     * @param metadata  the source metadata
     * @param offset    the record offset
     * @param data      the record data
     * @param variables the context variables
     *
     *
     * @return a new {@link InternalFilterContext} instance.
     */
    public static InternalFilterContext with(final SourceMetadata metadata,
                                             final FileInputOffset offset,
                                             final FileInputData data,
                                             final Map<String, SchemaAndValue> variables) {
        return new InternalFilterContext(metadata, offset, data, variables, null);
    }

    /**
     * Creates a new {@link InternalFilterContext} for the specified arguments.
     *
     * @param metadata  the source metadata
     * @param offset    the record offset
     * @param data      the record data
     * @param variables the context variables
     * @param exception the exception throw by prefious filter
     *
     * @return a new {@link InternalFilterContext} instance.
     */
    public static InternalFilterContext with(final SourceMetadata metadata,
                                             final FileInputOffset offset,
                                             final FileInputData data,
                                             final Map<String, SchemaAndValue> variables,
                                             final ExceptionContext exception) {
        return new InternalFilterContext(metadata, offset, data, variables, exception);
    }

    /**
     * Creates a new {@link InternalFilterContext} instance.
     *
     * @param metadata  the source metadata
     * @param offset    the record offset
     * @param data      the record data
     * @param variables the context variables
     * @param exception the exception throw by prefious filter
     */
    private InternalFilterContext(final SourceMetadata metadata,
                                  final FileInputOffset offset,
                                  final FileInputData data,
                                  final Map<String, SchemaAndValue> variables,
                                  final ExceptionContext exception) {
        super(variables);
        setSourceMetadata(Objects.requireNonNull(metadata, "metadata cannot be null"));
        setSourceOffset(Objects.requireNonNull(offset, "offset cannot be null"));
        if (data != null) setData(data);
        if (exception != null) setException(exception);
    }

    private void setSourceMetadata(final SourceMetadata metadata) {
        this.metadata = metadata;
        Struct struct = new Struct(FILE_SCHEMA)
                .put(FIELD_FILE_NAME, metadata.name())
                .put(FIELD_FILE_HASH, metadata.hash())
                .put(FIELD_FILE_LAST_MODIFIED, metadata.lastModified())
                .put(FIELD_FILE_ABSOLUTE_PATH, metadata.absolutePath())
                .put(FIELD_FILE_PATH, metadata.path())
                .put(FIELD_FILE_SIZE, metadata.size());
        set(FIELD_FILE, new SchemaAndValue(FILE_SCHEMA, struct));
    }

    private void setSourceOffset(final FileInputOffset offset) {
        this.offset = offset;
        set(FIELD_RECORD_OFFSET, new SchemaAndValue(SchemaBuilder.int64(), offset.startPosition()));
        set(FIELD_RECORD_LINE, new SchemaAndValue(SchemaBuilder.int64(), offset.rows()));
    }

    private void setData(final FileInputData data) {
        set(FIELD_RECORD_DATA, new SchemaAndValue(data.schema(), data.value()));
    }

    private void setException(final ExceptionContext exception) {
        this.exception = exception;
        Struct struct = new Struct(EXCEPTION_SCHEMA)
                .put(EXCEPTION_MESSAGE_FIELD, exception.message())
                .put(EXCEPTION_FILTER_FIELD, exception.filter());
        set(EXCEPTION_FIELD, new SchemaAndValue(EXCEPTION_SCHEMA, struct));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean has(final String name) {
        return super.has(name) || variables.get(FIELD_RECORD_DATA).schema().field(name) != null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SchemaAndValue get(final String name) {
        if (super.has(name)) {
            return variables.get(name);
        }
        else if (has(name) ){
            final SchemaAndValue sv = variables.get(FIELD_RECORD_DATA);
            final Field field = sv.schema().field(name);
            return new SchemaAndValue(field.schema(), ((Struct)sv.value()).get(name));
        }
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SourceMetadata metadata() {
        return metadata;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public FileInputOffset offset() {
        return offset;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ExceptionContext exception() {
        return exception;
    }
}

