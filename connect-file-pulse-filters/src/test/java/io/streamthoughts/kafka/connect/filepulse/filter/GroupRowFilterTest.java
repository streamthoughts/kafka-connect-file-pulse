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
import io.streamthoughts.kafka.connect.filepulse.source.FileInputData;
import io.streamthoughts.kafka.connect.filepulse.source.FileInputOffset;
import io.streamthoughts.kafka.connect.filepulse.source.SourceMetadata;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class GroupRowFilterTest {

    private static final String KEY_1 = "key1";
    private static final String KEY_2 = "key2";
    private static final String VALUE = "value";

    private static Schema SCHEMA =  SchemaBuilder.struct()
            .field(KEY_1, Schema.STRING_SCHEMA)
            .field(KEY_2, Schema.STRING_SCHEMA)
            .field(VALUE, Schema.STRING_SCHEMA)
            .build();

    private FilterContext context;

    private List<FileInputData> inputs;

    @Before
    public void setUp() {
        inputs = new ArrayList<>();
        inputs.addAll(generate(KEY_1, KEY_1, 2));
        inputs.addAll(generate(KEY_1, KEY_2, 2));
        inputs.addAll(generate(KEY_2, KEY_1, 2));
        inputs.addAll(generate(KEY_2, KEY_2, 2));

        SourceMetadata metadata = new SourceMetadata("", "", 0L, 0L, 0L, -1L);
        context = InternalFilterContext.with(metadata, FileInputOffset.empty());
    }

    @Test
    public void shouldAggregateRecordsWhenSingleFieldIsConfigured() {
        final GroupRowFilter filter = new GroupRowFilter();
        filter.configure(new HashMap<String, Object>(){{
            put(GroupRowFilterConfig.FIELDS_CONFIG, KEY_1);
            put(GroupRowFilterConfig.MAX_BUFFERED_RECORDS_CONFIG, "10");
        }});

        List<FileInputData> output = new LinkedList<>();
        Iterator<FileInputData> iterator = inputs.iterator();
        while (iterator.hasNext()) {
            output.addAll(filter.apply(context, iterator.next(), iterator.hasNext()).collect());
        }
        Assert.assertSame(2, output.size());
    }

    @Test
    public void shouldAggregateRecordsWhenMultipleFieldIsConfigured() {
        final GroupRowFilter filter = new GroupRowFilter();
        filter.configure(new HashMap<String, Object>(){{
            put(GroupRowFilterConfig.FIELDS_CONFIG, Arrays.asList(KEY_1, KEY_2));
            put(GroupRowFilterConfig.MAX_BUFFERED_RECORDS_CONFIG, "10");
        }});

        List<FileInputData> output = new LinkedList<>();
        Iterator<FileInputData> iterator = inputs.iterator();
        while (iterator.hasNext()) {
            output.addAll(filter.apply(context, iterator.next(), iterator.hasNext()).collect());
        }
        Assert.assertEquals(4, output.size());
    }

    @Test
    public void shouldForwardAggregateRecordIfMaxBufferedRecordIsReached() {
        final GroupRowFilter filter = new GroupRowFilter();
        filter.configure(new HashMap<String, Object>(){{
            put(GroupRowFilterConfig.FIELDS_CONFIG, KEY_1);
            put(GroupRowFilterConfig.MAX_BUFFERED_RECORDS_CONFIG, "1");
        }});

        List<FileInputData> output = new LinkedList<>();
        Iterator<FileInputData> iterator = inputs.iterator();
        while (iterator.hasNext()) {
            output.addAll(filter.apply(context, iterator.next(), iterator.hasNext()).collect());
        }
        Assert.assertEquals(8, output.size());
    }


    public List<FileInputData> generate(String key1, String key2, int num) {
        List<FileInputData> results = new ArrayList<>(num);
        for (int i = 0; i < num; i++) {
            Struct struct = new Struct(SCHEMA)
                    .put(KEY_1, key1)
                    .put(KEY_2, key2)
                    .put(VALUE, "val-" + i);
            results.add(new FileInputData(struct));
        }
        return results;
    }

    @Test
    public void  shouldGenerateDifferentKeysGivenSymmetricFields() {

        FileInputData struct1 = new FileInputData(new Struct(SCHEMA)
                .put(KEY_1, KEY_1)
                .put(KEY_2, KEY_2)
                .put(VALUE, "value"));

        FileInputData struct2 = new FileInputData(new Struct(SCHEMA)
                .put(KEY_1, KEY_2)
                .put(KEY_2, KEY_1)
                .put(VALUE, "value"));

        int hash1 = GroupRowFilter.extractKey(struct1, Arrays.asList(KEY_1, KEY_2));
        int hash2 = GroupRowFilter.extractKey(struct2, Arrays.asList(KEY_1, KEY_2));

        Assert.assertNotEquals(hash1, hash2);
    }
}