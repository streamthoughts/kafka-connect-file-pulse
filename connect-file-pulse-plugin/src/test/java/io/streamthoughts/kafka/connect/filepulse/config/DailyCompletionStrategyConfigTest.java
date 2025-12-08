/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.config;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.kafka.common.config.ConfigException;
import org.junit.Test;

/**
 * Test class for {@link DailyCompletionStrategyConfig}.
 */
public class DailyCompletionStrategyConfigTest {

    @Test
    public void testValidConfiguration() {
        Map<String, Object> props = new HashMap<>();
        props.put(DailyCompletionStrategyConfig.COMPLETION_SCHEDULE_TIME_CONFIG, "01:00:00");
        props.put(DailyCompletionStrategyConfig.COMPLETION_SCHEDULE_DATE_PATTERN_CONFIG, ".*?(\\d{4}-\\d{2}-\\d{2}).*");
        props.put(DailyCompletionStrategyConfig.COMPLETION_SCHEDULE_DATE_FORMAT_CONFIG, "yyyy-MM-dd");

        DailyCompletionStrategyConfig config = new DailyCompletionStrategyConfig(props);

        assertNotNull(config);
        assertEquals(LocalTime.of(1, 0, 0), config.scheduledCompletionTime());
        assertEquals(".*?(\\d{4}-\\d{2}-\\d{2}).*", config.datePattern().pattern());
        assertNotNull(config.dateFormatter());
    }

    @Test
    public void testConfigurationWithDefaults() {
        Map<String, Object> props = new HashMap<>();

        DailyCompletionStrategyConfig config = new DailyCompletionStrategyConfig(props);

        assertNotNull(config);
        assertEquals(LocalTime.of(0, 1, 0), config.scheduledCompletionTime());
        // Should use default pattern and format
        assertNotNull(config.datePattern());
        assertNotNull(config.dateFormatter());
    }

    @Test
    public void testValidTimeFormats() {
        String[] validTimes = {
            "00:00:00",
            "01:00:00",
            "12:00:00",
            "23:59:59",
            "06:30:15"
        };

        for (String time : validTimes) {
            Map<String, Object> props = new HashMap<>();
            props.put(DailyCompletionStrategyConfig.COMPLETION_SCHEDULE_TIME_CONFIG, time);

            DailyCompletionStrategyConfig config = new DailyCompletionStrategyConfig(props);
            LocalTime parsed = config.scheduledCompletionTime();
            assertNotNull("Time should be parsed: " + time, parsed);
        }
    }

    @Test(expected = ConfigException.class)
    public void testInvalidTimeFormat_NoSeconds() {
        Map<String, Object> props = new HashMap<>();
        props.put(DailyCompletionStrategyConfig.COMPLETION_SCHEDULE_TIME_CONFIG, "01:00");

        DailyCompletionStrategyConfig config = new DailyCompletionStrategyConfig(props);
        config.scheduledCompletionTime();
    }

    @Test(expected = ConfigException.class)
    public void testInvalidTimeFormat_InvalidHour() {
        Map<String, Object> props = new HashMap<>();
        props.put(DailyCompletionStrategyConfig.COMPLETION_SCHEDULE_TIME_CONFIG, "25:00:00");

        DailyCompletionStrategyConfig config = new DailyCompletionStrategyConfig(props);
        config.scheduledCompletionTime();
    }

    @Test(expected = ConfigException.class)
    public void testInvalidTimeFormat_InvalidMinute() {
        Map<String, Object> props = new HashMap<>();
        props.put(DailyCompletionStrategyConfig.COMPLETION_SCHEDULE_TIME_CONFIG, "12:60:00");

        DailyCompletionStrategyConfig config = new DailyCompletionStrategyConfig(props);
        config.scheduledCompletionTime();
    }

    @Test(expected = ConfigException.class)
    public void testInvalidTimeFormat_InvalidSecond() {
        Map<String, Object> props = new HashMap<>();
        props.put(DailyCompletionStrategyConfig.COMPLETION_SCHEDULE_TIME_CONFIG, "12:00:60");

        DailyCompletionStrategyConfig config = new DailyCompletionStrategyConfig(props);
        config.scheduledCompletionTime();
    }

    @Test(expected = ConfigException.class)
    public void testInvalidTimeFormat_InvalidString() {
        Map<String, Object> props = new HashMap<>();
        props.put(DailyCompletionStrategyConfig.COMPLETION_SCHEDULE_TIME_CONFIG, "not-a-time");

        DailyCompletionStrategyConfig config = new DailyCompletionStrategyConfig(props);
        config.scheduledCompletionTime();
    }

    @Test
    public void testValidPatterns() {
        String[] validPatterns = {
            ".*?(\\d{4}-\\d{2}-\\d{2}).*",
            ".*?(\\d{8}).*",
            "prefix-(\\d{4})-(\\d{2})-(\\d{2})-suffix",
            "(\\d{4})/(\\d{2})/(\\d{2})",
            ".*"
        };

        for (String pattern : validPatterns) {
            Map<String, Object> props = new HashMap<>();
            props.put(DailyCompletionStrategyConfig.COMPLETION_SCHEDULE_TIME_CONFIG, "01:00:00");
            props.put(DailyCompletionStrategyConfig.COMPLETION_SCHEDULE_DATE_PATTERN_CONFIG, pattern);

            DailyCompletionStrategyConfig config = new DailyCompletionStrategyConfig(props);
            Pattern compiled = config.datePattern();
            assertNotNull("Pattern should be compiled: " + pattern, compiled);
            assertEquals(pattern, compiled.pattern());
        }
    }

    @Test(expected = ConfigException.class)
    public void testInvalidPattern_UnclosedGroup() {
        Map<String, Object> props = new HashMap<>();
        props.put(DailyCompletionStrategyConfig.COMPLETION_SCHEDULE_TIME_CONFIG, "01:00:00");
        props.put(DailyCompletionStrategyConfig.COMPLETION_SCHEDULE_DATE_PATTERN_CONFIG, "(\\d{4}-\\d{2}-\\d{2}");

        DailyCompletionStrategyConfig config = new DailyCompletionStrategyConfig(props);
        config.datePattern();
    }

    @Test(expected = ConfigException.class)
    public void testInvalidPattern_InvalidRegex() {
        Map<String, Object> props = new HashMap<>();
        props.put(DailyCompletionStrategyConfig.COMPLETION_SCHEDULE_TIME_CONFIG, "01:00:00");
        props.put(DailyCompletionStrategyConfig.COMPLETION_SCHEDULE_DATE_PATTERN_CONFIG, "[invalid(regex");

        DailyCompletionStrategyConfig config = new DailyCompletionStrategyConfig(props);
        config.datePattern();
    }

    @Test
    public void testValidDateFormats() {
        String[] validFormats = {
            "yyyy-MM-dd",
            "yyyyMMdd",
            "yyyy/MM/dd",
            "dd-MM-yyyy",
            "MM/dd/yyyy",
            "yyyy.MM.dd",
            "yyyy-MM"
        };

        for (String format : validFormats) {
            Map<String, Object> props = new HashMap<>();
            props.put(DailyCompletionStrategyConfig.COMPLETION_SCHEDULE_TIME_CONFIG, "01:00:00");
            props.put(DailyCompletionStrategyConfig.COMPLETION_SCHEDULE_DATE_FORMAT_CONFIG, format);

            DailyCompletionStrategyConfig config = new DailyCompletionStrategyConfig(props);
            DateTimeFormatter formatter = config.dateFormatter();
            assertNotNull("Formatter should be created: " + format, formatter);
        }
    }

    @Test(expected = ConfigException.class)
    public void testInvalidDateFormat() {
        Map<String, Object> props = new HashMap<>();
        props.put(DailyCompletionStrategyConfig.COMPLETION_SCHEDULE_TIME_CONFIG, "01:00:00");
        props.put(DailyCompletionStrategyConfig.COMPLETION_SCHEDULE_DATE_FORMAT_CONFIG, "invalid-format-xxx");

        DailyCompletionStrategyConfig config = new DailyCompletionStrategyConfig(props);
        config.dateFormatter();
    }

    @Test(expected = ConfigException.class)
    public void testInvalidDateFormat_WrongLetters() {
        Map<String, Object> props = new HashMap<>();
        props.put(DailyCompletionStrategyConfig.COMPLETION_SCHEDULE_TIME_CONFIG, "01:00:00");
        props.put(DailyCompletionStrategyConfig.COMPLETION_SCHEDULE_DATE_FORMAT_CONFIG, "yyyy-PP-dd");

        DailyCompletionStrategyConfig config = new DailyCompletionStrategyConfig(props);
        config.dateFormatter();
    }

    @Test(expected = ConfigException.class)
    public void testInvalidDateFormat_UnknownLetter() {
        Map<String, Object> props = new HashMap<>();
        props.put(DailyCompletionStrategyConfig.COMPLETION_SCHEDULE_TIME_CONFIG, "01:00:00");
        props.put(DailyCompletionStrategyConfig.COMPLETION_SCHEDULE_DATE_FORMAT_CONFIG, "xyz-abc");

        DailyCompletionStrategyConfig config = new DailyCompletionStrategyConfig(props);
        config.dateFormatter();
    }

    @Test(expected = ConfigException.class)
    public void testInvalidDateFormat_UnmatchedBracket() {
        Map<String, Object> props = new HashMap<>();
        props.put(DailyCompletionStrategyConfig.COMPLETION_SCHEDULE_TIME_CONFIG, "01:00:00");
        props.put(DailyCompletionStrategyConfig.COMPLETION_SCHEDULE_DATE_FORMAT_CONFIG, "yyyy-MM-dd]");

        DailyCompletionStrategyConfig config = new DailyCompletionStrategyConfig(props);
        config.dateFormatter();
    }

    // ========== Combined Configuration Tests ==========

    @Test
    public void testCompactDateConfiguration() {
        Map<String, Object> props = new HashMap<>();
        props.put(DailyCompletionStrategyConfig.COMPLETION_SCHEDULE_TIME_CONFIG, "02:00:00");
        props.put(DailyCompletionStrategyConfig.COMPLETION_SCHEDULE_DATE_PATTERN_CONFIG, ".*?(\\d{8}).*");
        props.put(DailyCompletionStrategyConfig.COMPLETION_SCHEDULE_DATE_FORMAT_CONFIG, "yyyyMMdd");

        DailyCompletionStrategyConfig config = new DailyCompletionStrategyConfig(props);

        assertEquals(LocalTime.of(2, 0, 0), config.scheduledCompletionTime());
        assertEquals(".*?(\\d{8}).*", config.datePattern().pattern());
        assertNotNull(config.dateFormatter());
    }

    @Test
    public void testSlashDateConfiguration() {
        Map<String, Object> props = new HashMap<>();
        props.put(DailyCompletionStrategyConfig.COMPLETION_SCHEDULE_TIME_CONFIG, "03:30:00");
        props.put(DailyCompletionStrategyConfig.COMPLETION_SCHEDULE_DATE_PATTERN_CONFIG, ".*?(\\d{4})/(\\d{2})/(\\d{2}).*");
        props.put(DailyCompletionStrategyConfig.COMPLETION_SCHEDULE_DATE_FORMAT_CONFIG, "yyyy/MM/dd");

        DailyCompletionStrategyConfig config = new DailyCompletionStrategyConfig(props);

        assertEquals(LocalTime.of(3, 30, 0), config.scheduledCompletionTime());
        assertEquals(".*?(\\d{4})/(\\d{2})/(\\d{2}).*", config.datePattern().pattern());
        assertNotNull(config.dateFormatter());
    }

    @Test
    public void testReverseDateConfiguration() {
        Map<String, Object> props = new HashMap<>();
        props.put(DailyCompletionStrategyConfig.COMPLETION_SCHEDULE_TIME_CONFIG, "23:59:59");
        props.put(DailyCompletionStrategyConfig.COMPLETION_SCHEDULE_DATE_PATTERN_CONFIG, ".*?(\\d{2})-(\\d{2})-(\\d{4}).*");
        props.put(DailyCompletionStrategyConfig.COMPLETION_SCHEDULE_DATE_FORMAT_CONFIG, "dd-MM-yyyy");

        DailyCompletionStrategyConfig config = new DailyCompletionStrategyConfig(props);

        assertEquals(LocalTime.of(23, 59, 59), config.scheduledCompletionTime());
        assertEquals(".*?(\\d{2})-(\\d{2})-(\\d{4}).*", config.datePattern().pattern());
        assertNotNull(config.dateFormatter());
    }

    // ========== Edge Case Tests ==========

    @Test
    public void testMidnightTime() {
        Map<String, Object> props = new HashMap<>();
        props.put(DailyCompletionStrategyConfig.COMPLETION_SCHEDULE_TIME_CONFIG, "00:00:00");

        DailyCompletionStrategyConfig config = new DailyCompletionStrategyConfig(props);

        assertEquals(LocalTime.MIDNIGHT, config.scheduledCompletionTime());
    }

    @Test
    public void testEndOfDayTime() {
        Map<String, Object> props = new HashMap<>();
        props.put(DailyCompletionStrategyConfig.COMPLETION_SCHEDULE_TIME_CONFIG, "23:59:59");

        DailyCompletionStrategyConfig config = new DailyCompletionStrategyConfig(props);

        assertEquals(LocalTime.of(23, 59, 59), config.scheduledCompletionTime());
    }

    @Test
    public void testNoonTime() {
        Map<String, Object> props = new HashMap<>();
        props.put(DailyCompletionStrategyConfig.COMPLETION_SCHEDULE_TIME_CONFIG, "12:00:00");

        DailyCompletionStrategyConfig config = new DailyCompletionStrategyConfig(props);

        assertEquals(LocalTime.NOON, config.scheduledCompletionTime());
    }

    @Test
    public void testMultipleCapturingGroups() {
        Map<String, Object> props = new HashMap<>();
        props.put(DailyCompletionStrategyConfig.COMPLETION_SCHEDULE_TIME_CONFIG, "01:00:00");
        props.put(DailyCompletionStrategyConfig.COMPLETION_SCHEDULE_DATE_PATTERN_CONFIG, ".*?(\\d{4})-(\\d{2})-(\\d{2}).*");
        props.put(DailyCompletionStrategyConfig.COMPLETION_SCHEDULE_DATE_FORMAT_CONFIG, "yyyy-MM-dd");

        DailyCompletionStrategyConfig config = new DailyCompletionStrategyConfig(props);

        assertNotNull(config.datePattern());
        assertNotNull(config.dateFormatter());
    }
}