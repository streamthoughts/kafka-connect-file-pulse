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

import io.streamthoughts.kafka.connect.filepulse.config.GroupRowFilterConfig;
import io.streamthoughts.kafka.connect.filepulse.reader.FileInputRecord;
import io.streamthoughts.kafka.connect.filepulse.reader.RecordsIterable;
import io.streamthoughts.kafka.connect.filepulse.source.FileInputData;
import io.streamthoughts.kafka.connect.filepulse.source.FileInputOffset;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class GroupRowFilter extends AbstractRecordFilter<GroupRowFilter> {

    private GroupRowFilterConfig configs;

    private List<String> fields;

    private String target;

    private int maxBufferedRecords;

    private List<Struct> buffered = new LinkedList<>();

    private int lastObservedKey = -1;

    private FileInputOffset offset;

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(final Map<String, ?> configs) {
        super.configure(configs);
        this.configs = new GroupRowFilterConfig(configs);
        this.fields = this.configs.fields();
        this.target = this.configs.target();
        this.maxBufferedRecords = this.configs.maxBufferedRecords();
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
    public RecordsIterable<FileInputData> apply(final FilterContext context,
                                                final FileInputData record,
                                                final boolean hasNext) {

        final List<FileInputData> forward = new LinkedList<>();

        if (buffered.size() >= maxBufferedRecords) {
            forward.add(groupBufferedRecords());
        }

        final int key = extractKey(record, fields);
        if (mayForwardPreviousBufferedRecords(key)) {
            forward.add(groupBufferedRecords());
        }
        lastObservedKey = key;
        buffered.add(record.value());

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
    public RecordsIterable<FileInputRecord> flush() {
        if (buffered.size() == 0) RecordsIterable.empty();

        FileInputData data = groupBufferedRecords();
        return new RecordsIterable<>(new FileInputRecord(offset, data));
    }

    private boolean mayForwardPreviousBufferedRecords(final int key) {
        return lastObservedKey != -1 && lastObservedKey != key && hasRecordsBuffered();
    }

    private boolean hasRecordsBuffered() {
        return buffered.size() > 0;
    }

    private FileInputData groupBufferedRecords() {
        Struct peek = buffered.get(0);
        Schema schema = SchemaBuilder
                .struct()
                .field(this.target, SchemaBuilder.array(peek.schema()))
                .build();
        Struct struct = new Struct(schema);
        struct.put(target, new ArrayList<>(buffered));
        buffered.clear();
        return new FileInputData(struct);
    }

    static int extractKey(final FileInputData record, final List<String> fields) {
        final Struct struct = record.value();
        Object[] keys = new Object[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            keys[i] = struct.get(fields.get(i));
        }
        return Objects.hash(keys);
    }
}
