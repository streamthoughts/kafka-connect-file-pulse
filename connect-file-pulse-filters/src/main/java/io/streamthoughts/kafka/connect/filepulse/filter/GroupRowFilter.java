/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.filter;

import io.streamthoughts.kafka.connect.filepulse.config.GroupRowFilterConfig;
import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.reader.RecordsIterable;
import io.streamthoughts.kafka.connect.filepulse.source.FileRecord;
import io.streamthoughts.kafka.connect.filepulse.source.FileRecordOffset;
import io.streamthoughts.kafka.connect.filepulse.source.TypedFileRecord;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.common.config.ConfigDef;

public class GroupRowFilter extends AbstractRecordFilter<GroupRowFilter> {

    private List<String> fields;

    private String target;

    private int maxBufferedRecords;

    private final List<TypedStruct> buffered = new LinkedList<>();

    private int lastObservedKey = -1;

    private FileRecordOffset offset;

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(final Map<String, ?> configs) {
        super.configure(configs);

        final GroupRowFilterConfig config = new GroupRowFilterConfig(configs);

        this.fields = config.fields();
        this.target = config.target();
        this.maxBufferedRecords = config.maxBufferedRecords();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConfigDef configDef() {
        return GroupRowFilterConfig.configDef();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RecordsIterable<TypedStruct> apply(final FilterContext context,
                                              final TypedStruct record,
                                              final boolean hasNext) throws FilterException {

        final List<TypedStruct> forward = new LinkedList<>();

        if (buffered.size() >= maxBufferedRecords) {
            forward.add(groupBufferedRecords());
        }

        final int key = extractKey(record, fields);
        if (mayForwardPreviousBufferedRecords(key)) {
            forward.add(groupBufferedRecords());
        }
        lastObservedKey = key;
        buffered.add(record);

        if (!hasNext && hasRecordsBuffered()) {
            forward.add(groupBufferedRecords());
        }

        offset = context.offset();
        return new RecordsIterable<>(forward);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void clear() {
        buffered.clear();
        lastObservedKey = -1;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RecordsIterable<FileRecord<TypedStruct>> flush() {
        if (buffered.size() == 0) RecordsIterable.empty();

        TypedStruct data = groupBufferedRecords();
        return new RecordsIterable<>(new TypedFileRecord(offset, data));
    }

    private boolean mayForwardPreviousBufferedRecords(final int key) {
        return lastObservedKey != -1 && lastObservedKey != key && hasRecordsBuffered();
    }

    private boolean hasRecordsBuffered() {
        return buffered.size() > 0;
    }

    private TypedStruct groupBufferedRecords() {
        final TypedStruct struct = TypedStruct.create();
        struct.put(target, new ArrayList<>(buffered));
        buffered.clear();
        return struct;
    }

    static int extractKey(final TypedStruct record, final List<String> fields) {
        Object[] keys = new Object[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            keys[i] = record.get(fields.get(i));
        }
        return Objects.hash(keys);
    }
}
