/*
 * Copyright 2019-2021 StreamThoughts.
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
package io.streamthoughts.kafka.connect.filepulse.config;

import io.streamthoughts.kafka.connect.filepulse.filter.AppendFilter;
import io.streamthoughts.kafka.connect.filepulse.filter.RecordFilter;
import io.streamthoughts.kafka.connect.filepulse.fs.DefaultTaskFileURIProvider;
import io.streamthoughts.kafka.connect.filepulse.fs.FileListFilter;
import io.streamthoughts.kafka.connect.filepulse.fs.FileSystemListing;
import io.streamthoughts.kafka.connect.filepulse.fs.Storage;
import io.streamthoughts.kafka.connect.filepulse.fs.reader.LocalRowFileInputReader;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectMeta;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

public class SourceTaskConfigTest {

    @Test
    public void should_configure_filter_given_on_failure() {
        final SourceTaskConfig config = new SourceTaskConfig(new HashMap<>() {{
            put(CommonSourceConfig.OUTPUT_TOPIC_CONFIG, "Test");
            put(CommonSourceConfig.FS_LISTING_CLASS_CONFIG, MockFileSystemListing.class.getName());
            put(CommonSourceConfig.TASKS_FILE_READER_CLASS_CONFIG, LocalRowFileInputReader.class.getName());
            put(DefaultTaskFileURIProvider.Config.FILE_OBJECT_URIS_CONFIG, "/tmp");
            put(CommonSourceConfig.FILTER_CONFIG, "Test");
            put(CommonSourceConfig.FILTER_CONFIG + ".Test.type", AppendFilter.class.getName());
            put(CommonSourceConfig.FILTER_CONFIG + ".Test." + AppendFilterConfig.APPEND_FIELD_CONFIG, "field");
            put(CommonSourceConfig.FILTER_CONFIG + ".Test." + AppendFilterConfig.APPEND_VALUE_CONFIG, "value");
            put(CommonSourceConfig.FILTER_CONFIG + ".Test." + AppendFilterConfig.ON_FAILURE_CONFIG, "Failure");
            put(CommonSourceConfig.FILTER_CONFIG + ".Failure.type", AppendFilter.class.getName());
            put(CommonSourceConfig.FILTER_CONFIG + ".Failure." + AppendFilterConfig.APPEND_FIELD_CONFIG, "error");
            put(CommonSourceConfig.FILTER_CONFIG + ".Failure." + AppendFilterConfig.APPEND_VALUE_CONFIG, "value");
        }});

        final List<RecordFilter> filters = config.filters();
        Assert.assertEquals(1, filters.size());
        Assert.assertNotNull(filters.get(0).onFailure());
    }

    static class MockFileSystemListing implements FileSystemListing {

        @Override
        public void configure(Map configs) {

        }

        @Override
        public Collection<FileObjectMeta> listObjects() {
            return null;
        }

        @Override
        public void setFilter(FileListFilter filter) {

        }

        @Override
        public Storage storage() {
            return null;
        }
    }
}