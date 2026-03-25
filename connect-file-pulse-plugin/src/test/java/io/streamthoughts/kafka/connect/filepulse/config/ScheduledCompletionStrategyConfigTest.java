/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.config;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import io.streamthoughts.kafka.connect.filepulse.source.CompletionPeriod;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.kafka.common.config.ConfigException;
import org.junit.Test;

/**
 * Test class for {@link ScheduledCompletionStrategyConfig}.
 */
public class ScheduledCompletionStrategyConfigTest {

    // -------------------------------------------------------------------------
    // Defaults
    // -------------------------------------------------------------------------

    @Test
    public void testDefaultConfiguration() {
        ScheduledCompletionStrategyConfig config = new ScheduledCompletionStrategyConfig(Map.of());

        assertEquals(LocalTime.of(0, 1, 0), config.scheduledCompletionTime());
        assertNotNull(config.datePattern());
        assertNotNull(config.dateFormatter());
        assertEquals(CompletionPeriod.DAILY, config.completionPeriod());
    }

    // -------------------------------------------------------------------------
    // Period
    // -------------------------------------------------------------------------

    @Test
    public void testPeriodDaily() {
        Map<String, Object> props = new HashMap<>();
        props.put(ScheduledCompletionStrategyConfig.COMPLETION_SCHEDULE_PERIOD_CONFIG, "DAILY");

        ScheduledCompletionStrategyConfig config = new ScheduledCompletionStrategyConfig(props);
        assertEquals(CompletionPeriod.DAILY, config.completionPeriod());
    }

    @Test
    public void testPeriodWeekly() {
        Map<String, Object> props = new HashMap<>();
        props.put(ScheduledCompletionStrategyConfig.COMPLETION_SCHEDULE_PERIOD_CONFIG, "WEEKLY");

        ScheduledCompletionStrategyConfig config = new ScheduledCompletionStrategyConfig(props);
        assertEquals(CompletionPeriod.WEEKLY, config.completionPeriod());
    }

    @Test
    public void testPeriodMonthly() {
        Map<String, Object> props = new HashMap<>();
        props.put(ScheduledCompletionStrategyConfig.COMPLETION_SCHEDULE_PERIOD_CONFIG, "MONTHLY");

        ScheduledCompletionStrategyConfig config = new ScheduledCompletionStrategyConfig(props);
        assertEquals(CompletionPeriod.MONTHLY, config.completionPeriod());
    }

    @Test(expected = ConfigException.class)
    public void testInvalidPeriod() {
        Map<String, Object> props = new HashMap<>();
        props.put(ScheduledCompletionStrategyConfig.COMPLETION_SCHEDULE_PERIOD_CONFIG, "HOURLY");

        new ScheduledCompletionStrategyConfig(props).completionPeriod();
    }

    // -------------------------------------------------------------------------
    // Time
    // -------------------------------------------------------------------------

    @Test
    public void testValidTime() {
        Map<String, Object> props = new HashMap<>();
        props.put(ScheduledCompletionStrategyConfig.COMPLETION_SCHEDULE_TIME_CONFIG, "06:30:15");

        ScheduledCompletionStrategyConfig config = new ScheduledCompletionStrategyConfig(props);
        assertEquals(LocalTime.of(6, 30, 15), config.scheduledCompletionTime());
    }

    @Test(expected = ConfigException.class)
    public void testInvalidTime_NoSeconds() {
        Map<String, Object> props = new HashMap<>();
        props.put(ScheduledCompletionStrategyConfig.COMPLETION_SCHEDULE_TIME_CONFIG, "01:00");

        new ScheduledCompletionStrategyConfig(props).scheduledCompletionTime();
    }

    @Test(expected = ConfigException.class)
    public void testInvalidTime_InvalidHour() {
        Map<String, Object> props = new HashMap<>();
        props.put(ScheduledCompletionStrategyConfig.COMPLETION_SCHEDULE_TIME_CONFIG, "25:00:00");

        new ScheduledCompletionStrategyConfig(props).scheduledCompletionTime();
    }

    @Test(expected = ConfigException.class)
    public void testInvalidTime_InvalidString() {
        Map<String, Object> props = new HashMap<>();
        props.put(ScheduledCompletionStrategyConfig.COMPLETION_SCHEDULE_TIME_CONFIG, "not-a-time");

        new ScheduledCompletionStrategyConfig(props).scheduledCompletionTime();
    }

    // -------------------------------------------------------------------------
    // Date pattern
    // -------------------------------------------------------------------------

    @Test
    public void testValidPattern() {
        Map<String, Object> props = new HashMap<>();
        props.put(ScheduledCompletionStrategyConfig.COMPLETION_SCHEDULE_DATE_PATTERN_CONFIG, ".*?(\\d{8}).*");

        ScheduledCompletionStrategyConfig config = new ScheduledCompletionStrategyConfig(props);
        Pattern pattern = config.datePattern();
        assertNotNull(pattern);
        assertEquals(".*?(\\d{8}).*", pattern.pattern());
    }

    @Test(expected = ConfigException.class)
    public void testInvalidPattern() {
        Map<String, Object> props = new HashMap<>();
        props.put(ScheduledCompletionStrategyConfig.COMPLETION_SCHEDULE_DATE_PATTERN_CONFIG, "[invalid(regex");

        new ScheduledCompletionStrategyConfig(props).datePattern();
    }

    // -------------------------------------------------------------------------
    // Date format
    // -------------------------------------------------------------------------

    @Test
    public void testValidDateFormat() {
        Map<String, Object> props = new HashMap<>();
        props.put(ScheduledCompletionStrategyConfig.COMPLETION_SCHEDULE_DATE_FORMAT_CONFIG, "yyyyMMdd");

        ScheduledCompletionStrategyConfig config = new ScheduledCompletionStrategyConfig(props);
        DateTimeFormatter formatter = config.dateFormatter();
        assertNotNull(formatter);
    }

    @Test(expected = ConfigException.class)
    public void testInvalidDateFormat() {
        Map<String, Object> props = new HashMap<>();
        props.put(ScheduledCompletionStrategyConfig.COMPLETION_SCHEDULE_DATE_FORMAT_CONFIG, "invalid-format-xxx");

        new ScheduledCompletionStrategyConfig(props).dateFormatter();
    }
}
