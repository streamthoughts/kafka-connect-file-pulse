/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.filter;

import io.streamthoughts.kafka.connect.filepulse.config.CommonFilterConfig;
import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.transform.GrokConfig;
import java.util.Arrays;
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

    @Test
    public void testGivenTargetField() {
        configs.put(GrokConfig.GROK_PATTERN_CONFIG, GROK_NAMED_CAPTURED_PATTERN);
        configs.put("target", "logData");
        filter.configure(configs, alias -> null);
        List<TypedStruct> results = filter.apply(null, DATA, false).collect();

        Assert.assertEquals(1, results.size());
        TypedStruct struct = results.get(0);
        
        // Original message field should still exist
        Assert.assertEquals(INPUT, struct.getString("message"));
        
        // Extracted data should be in the target field
        Assert.assertTrue(struct.exists("logData"));
        TypedStruct logData = struct.getStruct("logData");
        Assert.assertNotNull(logData);
        Assert.assertEquals("1970-01-01 00:00:00,000", logData.getString("timestamp"));
        Assert.assertEquals("INFO", logData.getString("level"));
        // Inside the target struct, message is just the extracted value (no merging with original)
        Assert.assertEquals("a dummy log message", logData.getString("message"));
    }

    @Test
    public void testGivenTargetFieldWithOverwrite() {
        configs.put(GrokConfig.GROK_PATTERN_CONFIG, GROK_NAMED_CAPTURED_PATTERN);
        configs.put("target", "parsed");
        configs.put(CommonFilterConfig.FILTER_OVERWRITE_CONFIG, "message");
        filter.configure(configs, alias -> null);
        List<TypedStruct> results = filter.apply(null, DATA, false).collect();

        Assert.assertEquals(1, results.size());
        TypedStruct struct = results.get(0);
        
        // Original message field should still exist
        Assert.assertEquals(INPUT, struct.getString("message"));
        
        // Extracted data should be in the target field with overwrite applied
        Assert.assertTrue(struct.exists("parsed"));
        TypedStruct parsed = struct.getStruct("parsed");
        Assert.assertNotNull(parsed);
        Assert.assertEquals("1970-01-01 00:00:00,000", parsed.getString("timestamp"));
        Assert.assertEquals("INFO", parsed.getString("level"));
        // With overwrite, message should be a string, not an array
        Assert.assertEquals("a dummy log message", parsed.getString("message"));
    }

    @Test
    public void testGivenArraySourceWithNestedTarget() {
        TypedStruct record = TypedStruct.create();
        record.put(
            "logData",
            Arrays.asList(
                "1970-01-01 00:00:00,000 INFO first log message",
                "1970-01-01 00:00:01,000 WARN second log message",
                "1970-01-01 00:00:02,000 ERROR third log message"));

        configs.put(GrokConfig.GROK_PATTERN_CONFIG, GROK_NAMED_CAPTURED_PATTERN);
        configs.put(CommonFilterConfig.FILTER_SOURCE_FIELD_CONFIG, "logData");
        configs.put("target", "parsed.records");
        filter.configure(configs, alias -> null);

        List<TypedStruct> results = filter.apply(null, record, false).collect();
        Assert.assertEquals(1, results.size());
        TypedStruct struct = results.get(0);

        Assert.assertTrue(struct.exists("parsed"));
        TypedStruct parsed = struct.getStruct("parsed");
        Assert.assertNotNull(parsed);
        Assert.assertTrue(parsed.exists("records"));
        List<Object> records = parsed.getArray("records");
        Assert.assertEquals(3, records.size());

        TypedStruct first = (TypedStruct) records.get(0);
        Assert.assertEquals("1970-01-01 00:00:00,000", first.getString("timestamp"));
        Assert.assertEquals("INFO", first.getString("level"));
        Assert.assertEquals("first log message", first.getString("message"));

        TypedStruct last = (TypedStruct) records.get(2);
        Assert.assertEquals("1970-01-01 00:00:02,000", last.getString("timestamp"));
        Assert.assertEquals("ERROR", last.getString("level"));
        Assert.assertEquals("third log message", last.getString("message"));
    }

    @Test(expected = FilterException.class)
    public void testGivenArraySourceWithInvalidElementTypeShouldFail() {
        TypedStruct record = TypedStruct.create().put(
            "logs",
            Arrays.asList("1970-01-01 00:00:00,000 INFO first log message", 1));

        configs.put(GrokConfig.GROK_PATTERN_CONFIG, GROK_NAMED_CAPTURED_PATTERN);
        configs.put(CommonFilterConfig.FILTER_SOURCE_FIELD_CONFIG, "logs");
        filter.configure(configs, alias -> null);

        filter.apply(null, record, false);
    }

        @Test(expected = FilterException.class)
        public void testGivenArraySourceWithNonMatchingEntryShouldFail() {
        TypedStruct record = TypedStruct.create().put(
            "logs",
            Arrays.asList(
                "1970-01-01 00:00:00,000 INFO first log message",
                "INVALID"));

        configs.put(GrokConfig.GROK_PATTERN_CONFIG, GROK_NAMED_CAPTURED_PATTERN);
        configs.put(CommonFilterConfig.FILTER_SOURCE_FIELD_CONFIG, "logs");
        filter.configure(configs, alias -> null);

        filter.apply(null, record, false);
        }
}