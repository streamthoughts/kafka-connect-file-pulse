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
package io.streamthoughts.kafka.connect.filepulse.filter;

import io.streamthoughts.kafka.connect.filepulse.config.CommonFilterConfig;
import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.transform.GrokConfig;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class GrokFilterTest {

    private static final String GROK_NAMED_CAPTURED_PATTERN = "%{TIMESTAMP_ISO8601:timestamp} %{LOGLEVEL:level} %{GREEDYDATA:message}";
    private static final String GROK_PATTERN = "%{TIMESTAMP_ISO8601} %{LOGLEVEL} %{GREEDYDATA}";

    private GrokFilter filter;

    private Map<String, String> configs;

    private static final String INPUT = "1970-01-01 00:00:00,000 INFO a dummy log message\n";

    private static final TypedStruct DATA = TypedStruct.create().put("message", INPUT);

    @Before
    public void setUp() {
        filter = new GrokFilter();
        configs = new HashMap<>();
    }

    @Test
    public void testGivenDefaultProperties() {
        configs.put(GrokConfig.GROK_PATTERN_CONFIG, GROK_NAMED_CAPTURED_PATTERN);
        filter.configure(configs, alias -> null);
        List<TypedStruct> results = filter.apply(null, DATA, false).collect();

        Assert.assertEquals(1, results.size());
        TypedStruct struct = results.get(0);
        Assert.assertEquals("1970-01-01 00:00:00,000", struct.getString("timestamp"));
        Assert.assertEquals("INFO", struct.getString("level"));
        Assert.assertEquals(2, struct.getArray("message").size());
        Assert.assertEquals(INPUT, struct.getArray("message").get(0));
        Assert.assertEquals("a dummy log message", struct.getArray("message").get(1));
    }

    @Test
    public void testGivenOverwriteProperty() {
        configs.put(GrokConfig.GROK_PATTERN_CONFIG, GROK_NAMED_CAPTURED_PATTERN);
        configs.put(CommonFilterConfig.FILTER_OVERWRITE_CONFIG, "message");
        filter.configure(configs, alias -> null);
        List<TypedStruct> results = filter.apply(null, DATA, false).collect();

        Assert.assertEquals(1, results.size());
        TypedStruct struct = results.get(0);
        Assert.assertEquals("1970-01-01 00:00:00,000", struct.getString("timestamp"));
        Assert.assertEquals("INFO", struct.getString("level"));
        Assert.assertEquals("a dummy log message", struct.getString("message"));
    }

    @Test(expected = FilterException.class)
    public void testGivenNotMatchingInput() {
        configs.put(GrokConfig.GROK_PATTERN_CONFIG, GROK_NAMED_CAPTURED_PATTERN);
        configs.put(CommonFilterConfig.FILTER_OVERWRITE_CONFIG, "message");
        filter.configure(configs, alias -> null);
        filter.apply(null, TypedStruct.create().put("message", "BAD INPUT"), false);
    }

    @Test
    public void testGivenPatternWithNoGroupWhenCapturedNameOnlyIsFalse() {
        configs.put(GrokConfig.GROK_PATTERN_CONFIG, GROK_PATTERN);
        configs.put(CommonFilterConfig.FILTER_OVERWRITE_CONFIG, "message");
        configs.put(GrokConfig.GROK_NAMED_CAPTURES_ONLY_CONFIG, "false");
        filter.configure(configs, alias -> null);
        List<TypedStruct> results = filter.apply(null, DATA, false).collect();

        Assert.assertEquals(1, results.size());
        TypedStruct struct = results.get(0);
        Assert.assertEquals("1970-01-01 00:00:00,000", struct.getString("TIMESTAMP_ISO8601"));
        Assert.assertEquals("INFO", struct.getString("LOGLEVEL"));
        Assert.assertEquals("a dummy log message", struct.getString("GREEDYDATA"));
    }
}