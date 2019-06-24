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

import io.streamthoughts.kafka.connect.filepulse.filter.config.CommonFilterConfig;
import io.streamthoughts.kafka.connect.filepulse.config.GrokFilterConfig;
import io.streamthoughts.kafka.connect.filepulse.source.SourceMetadata;
import io.streamthoughts.kafka.connect.filepulse.source.FileInputData;
import io.streamthoughts.kafka.connect.filepulse.source.FileInputOffset;
import org.apache.kafka.connect.data.Struct;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class GrokFilterTest {

    private static final String GROK_NAMED_CAPTURED_PATTERN = "%{TIMESTAMP_ISO8601:timestamp} %{LOGLEVEL:level} %{GREEDYDATA:message}";
    private static final String GROK_PATTERN = "%{TIMESTAMP_ISO8601} %{LOGLEVEL} %{GREEDYDATA}";

    private FilterContext context;

    private GrokFilter filter;

    private Map<String, String> configs;

    private static final String INPUT = "1970-01-01 00:00:00,000 INFO a dummy log message\n";

    private static final FileInputData DATA = FileInputData.defaultStruct(INPUT);

    @Before
    public void setUp() {
        filter = new GrokFilter();
        configs = new HashMap<>();
        SourceMetadata metadata = new SourceMetadata("", "", 0L, 0L, 0L, -1L);
        context = InternalFilterContext.with(metadata, FileInputOffset.empty());
    }

    @Test
    public void testGivenDefaultProperties() {
        configs.put(GrokFilterConfig.GROK_ROW_PATTERN_CONFIG, GROK_NAMED_CAPTURED_PATTERN);
        filter.configure(configs);
        List<Struct> results = filter.apply(null, DATA, false)
                .stream()
                .map(FileInputData::value)
                .collect(Collectors.toList());

        Assert.assertEquals(1, results.size());
        Struct struct = results.get(0);
        Assert.assertEquals("1970-01-01 00:00:00,000", struct.getString("timestamp"));
        Assert.assertEquals("INFO", struct.getString("level"));
        Assert.assertEquals(2, struct.getArray("message").size());
        Assert.assertEquals(INPUT, struct.getArray("message").get(0));
        Assert.assertEquals("a dummy log message", struct.getArray("message").get(1));
    }

    @Test
    public void testGivenOverwriteProperty() {
        configs.put(GrokFilterConfig.GROK_ROW_PATTERN_CONFIG, GROK_NAMED_CAPTURED_PATTERN);
        configs.put(CommonFilterConfig.FILTER_OVERWRITE_CONFIG, "message");
        filter.configure(configs);
        List<Struct> results = filter.apply(null, DATA, false)
                .stream()
                .map(o -> (Struct) o.value())
                .collect(Collectors.toList());

        Assert.assertEquals(1, results.size());
        Struct struct = results.get(0);
        Assert.assertEquals("1970-01-01 00:00:00,000", struct.getString("timestamp"));
        Assert.assertEquals("INFO", struct.getString("level"));
        Assert.assertEquals("a dummy log message", struct.getString("message"));
    }

    @Test(expected = FilterException.class)
    public void testGivenNotMatchingInput() {
        configs.put(GrokFilterConfig.GROK_ROW_PATTERN_CONFIG, GROK_NAMED_CAPTURED_PATTERN);
        configs.put(CommonFilterConfig.FILTER_OVERWRITE_CONFIG, "message");
        filter.configure(configs);
        filter.apply(null,  FileInputData.defaultStruct("BAD INPUT"), false);
    }

    @Test
    public void testGivenPatternWithNoGroupWhenCapturedNameOnlyIsFalse() {
        configs.put(GrokFilterConfig.GROK_ROW_PATTERN_CONFIG, GROK_PATTERN);
        configs.put(CommonFilterConfig.FILTER_OVERWRITE_CONFIG, "message");
        configs.put(GrokFilterConfig.GROK_ROW_NAMED_CAPTURES_ONLY_CONFIG, "false");
        filter.configure(configs);
        List<Struct> results = filter.apply(null, DATA, false)
                .stream()
                .map(o -> (Struct) o.value())
                .collect(Collectors.toList());

        Assert.assertEquals(1, results.size());
        Struct struct = results.get(0);
        Assert.assertEquals("1970-01-01 00:00:00,000", struct.getString("TIMESTAMP_ISO8601"));
        Assert.assertEquals("INFO", struct.getString("LOGLEVEL"));
        Assert.assertEquals("a dummy log message", struct.getString("GREEDYDATA"));
    }
}