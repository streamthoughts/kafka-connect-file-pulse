/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.config;

import io.streamthoughts.kafka.connect.filepulse.MockFileSystemListing;
import io.streamthoughts.kafka.connect.filepulse.filter.AppendFilter;
import io.streamthoughts.kafka.connect.filepulse.filter.RecordFilter;
import io.streamthoughts.kafka.connect.filepulse.fs.DefaultTaskFileURIProvider;
import io.streamthoughts.kafka.connect.filepulse.fs.reader.LocalRowFileInputReader;
import java.util.HashMap;
import java.util.List;
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
}