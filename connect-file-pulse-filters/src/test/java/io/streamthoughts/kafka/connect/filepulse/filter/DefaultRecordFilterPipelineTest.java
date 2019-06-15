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

import io.streamthoughts.kafka.connect.filepulse.config.AppendFilterConfig;
import io.streamthoughts.kafka.connect.filepulse.config.RenameFilterConfig;
import io.streamthoughts.kafka.connect.filepulse.offset.SourceMetadata;
import io.streamthoughts.kafka.connect.filepulse.reader.FileInputRecord;
import io.streamthoughts.kafka.connect.filepulse.reader.RecordsIterable;
import io.streamthoughts.kafka.connect.filepulse.source.FileInputContext;
import io.streamthoughts.kafka.connect.filepulse.source.FileInputData;
import io.streamthoughts.kafka.connect.filepulse.source.FileInputOffset;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;

public class DefaultRecordFilterPipelineTest {


    private static final String DEFAULT_FIELD = "foo";
    private static final String DEFAULT_FIELD_VALUE = "dummy";

    private FileInputContext context = new FileInputContext(
            new SourceMetadata("", "", 0L, 0L, 0L, -1L)
    );

    private final RecordsIterable<FileInputRecord> RECORD = new RecordsIterable<>(
            new FileInputRecord(
                    FileInputOffset.empty(),
                    new FileInputData(new Struct(SchemaBuilder.struct().field(DEFAULT_FIELD, SchemaBuilder.string()))
                            .put("foo", DEFAULT_FIELD_VALUE))
            )
    );

    @Test
    public void testGivenSuccessfulPipeline() {
        AppendFilter append = new AppendFilter();

        append.configure(new HashMap<String, String>(){{
            put(AppendFilterConfig.APPEND_FIELD_CONFIG, "error");
            put(AppendFilterConfig.APPEND_VALUE_CONFIG, "missing field");
        }});

        RenameFilter rename = new RenameFilter();
        rename.configure(new HashMap<String, String>(){{
            put(RenameFilterConfig.RENAME_FIELD_CONFIG, "foo");
            put(RenameFilterConfig.RENAME_TARGET_CONFIG, "bar");
            put(RenameFilterConfig.RENAME_IGNORE_MISSING_CONFIG, "false");
        }});

        rename.withOnFailure(new DefaultRecordFilterPipeline(Collections.singletonList(append)));

        DefaultRecordFilterPipeline pipeline = new DefaultRecordFilterPipeline(Collections.singletonList(rename));
        pipeline.init(context);

        RecordsIterable<FileInputRecord> iterable = pipeline.apply(RECORD, false);
        List<FileInputRecord> records = iterable.collect();

        Assert.assertNotNull(records);
        Assert.assertEquals(1, records.size());
        FileInputRecord record = records.get(0);
        Assert.assertEquals(DEFAULT_FIELD_VALUE, record.data().value().getString("bar"));
    }

    @Test
    public void testGivenFailurePipelineWhenErrorOccurred() {

        AppendFilter append = new AppendFilter();

        append.configure(new HashMap<String, String>(){{
            put(AppendFilterConfig.APPEND_FIELD_CONFIG, "error");
            put(AppendFilterConfig.APPEND_VALUE_CONFIG, "{{ $error.message }}");
        }});

        RenameFilter rename = new RenameFilter();
        rename.configure(new HashMap<String, String>(){{
            put(RenameFilterConfig.RENAME_FIELD_CONFIG, "bar"); // This field is not declare
            put(RenameFilterConfig.RENAME_TARGET_CONFIG, "foo");
            put(RenameFilterConfig.RENAME_IGNORE_MISSING_CONFIG, "false");
        }});

        rename.withOnFailure(new DefaultRecordFilterPipeline(Collections.singletonList(append)));

        DefaultRecordFilterPipeline pipeline = new DefaultRecordFilterPipeline(Collections.singletonList(rename));
        pipeline.init(context);

        RecordsIterable<FileInputRecord> iterable = pipeline.apply(RECORD, false);
        List<FileInputRecord> records = iterable.collect();

        Assert.assertNotNull(records);
        Assert.assertEquals(1, records.size());
        FileInputRecord record = records.get(0);
        Assert.assertEquals("Cannot find field with name 'bar'", record.data().getFirstValueForField("error"));
    }

    @Test(expected = FilterException.class)
    public void testGivenNoFailurePipelineWhenErrorOccurredAndIsNotIgnored() {

        RenameFilter rename = new RenameFilter();
        rename.configure(new HashMap<String, String>(){{
            put(RenameFilterConfig.RENAME_FIELD_CONFIG, "bar"); // This field is not declare
            put(RenameFilterConfig.RENAME_TARGET_CONFIG, "foo");
            put(RenameFilterConfig.RENAME_IGNORE_MISSING_CONFIG, "false");
        }});

        DefaultRecordFilterPipeline pipeline = new DefaultRecordFilterPipeline(Collections.singletonList(rename));
        pipeline.init(context);

        pipeline.apply(RECORD, false); // This throw exception
    }

    @Test
    public void testGivenNoFailurePipelineWhenErrorOccurredAndIsIgnored() {

        RenameFilter rename = new RenameFilter();
        rename.configure(new HashMap<String, String>(){{
            put(RenameFilterConfig.RENAME_FIELD_CONFIG, "bar"); // This field is not declare
            put(RenameFilterConfig.RENAME_TARGET_CONFIG, "foo");
            put(RenameFilterConfig.RENAME_IGNORE_MISSING_CONFIG, "true");
        }});

        DefaultRecordFilterPipeline pipeline = new DefaultRecordFilterPipeline(Collections.singletonList(rename));
        pipeline.init(context);

        RecordsIterable<FileInputRecord> iterable = pipeline.apply(RECORD, false);
        List<FileInputRecord> records = iterable.collect();
        Assert.assertNotNull(records);
        Assert.assertEquals(1, records.size());
        FileInputRecord record = records.get(0);
        Assert.assertEquals(DEFAULT_FIELD_VALUE, record.data().getFirstValueForField(DEFAULT_FIELD));
    }

}