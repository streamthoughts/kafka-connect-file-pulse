/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.config;

import io.streamthoughts.kafka.connect.filepulse.source.DailyCompletionStrategy;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

/**
 * Configuration for {@link DailyCompletionStrategy}.
 */
public class DailyCompletionStrategyConfig extends AbstractConfig {

    public static final String COMPLETION_SCHEDULE_TIME_CONFIG = "daily.completion.schedule.time";
    private static final String COMPLETION_SCHEDULE_TIME_DOC =
        "Time to complete files in HH:mm:ss format (e.g., '00:01:00'). " +
        "Files will be marked as COMPLETED when the current time passes this scheduled time " +
        "for the date extracted from the filename. Uses the system default timezone.";
    private static final String COMPLETION_SCHEDULE_TIME_DEFAULT = "00:01:00";

    public static final String COMPLETION_SCHEDULE_DATE_PATTERN_CONFIG = "daily.completion.schedule.date.pattern";
    private static final String COMPLETION_SCHEDULE_DATE_PATTERN_DOC =
        "Regex pattern to extract date from filename. The pattern should contain capturing groups " +
        "that match the date components. For example: '.*?(\\d{4}-\\d{2}-\\d{2}).*' to match dates " +
        "like '2025-12-08' in filenames like 'logs-2025-12-08.log'.";
    private static final String COMPLETION_SCHEDULE_DATE_PATTERN_DEFAULT = ".*?(\\d{4}-\\d{2}-\\d{2}).*";

    public static final String COMPLETION_SCHEDULE_DATE_FORMAT_CONFIG = "daily.completion.schedule.date.format";
    private static final String COMPLETION_SCHEDULE_DATE_FORMAT_DOC =
        "Date format pattern used to parse the date extracted from the filename. " +
        "Must match the date format in the filename (e.g., 'yyyy-MM-dd' for '2025-12-08', " +
        "'yyyyMMdd' for '20251208'). See Java DateTimeFormatter for supported patterns.";
    private static final String COMPLETION_SCHEDULE_DATE_FORMAT_DEFAULT = "yyyy-MM-dd";

    /**
     * Creates a new {@link DailyCompletionStrategyConfig} instance.
     *
     * @param originals the configuration properties
     */
    public DailyCompletionStrategyConfig(final Map<?, ?> originals) {
        super(configDef(), originals);
    }

    /**
     * Get the scheduled completion time.
     *
     * @return the time at which files should be completed
     */
    public LocalTime scheduledCompletionTime() {
        String timeStr = getString(COMPLETION_SCHEDULE_TIME_CONFIG);
        try {
            return LocalTime.parse(timeStr, DateTimeFormatter.ofPattern("HH:mm:ss"));
        } catch (DateTimeParseException e) {
            throw new ConfigException(
                COMPLETION_SCHEDULE_TIME_CONFIG,
                timeStr,
                "Invalid time format. Expected format: HH:mm:ss (e.g., '23:59:59')"
            );
        }
    }

    /**
     * Get the compiled regex pattern for extracting dates from filenames.
     *
     * @return the compiled pattern
     */
    public Pattern datePattern() {
        String patternStr = getString(COMPLETION_SCHEDULE_DATE_PATTERN_CONFIG);
        try {
            return Pattern.compile(patternStr);
        } catch (PatternSyntaxException e) {
            throw new ConfigException(
                COMPLETION_SCHEDULE_DATE_PATTERN_CONFIG,
                patternStr,
                "Invalid regex pattern: " + e.getMessage()
            );
        }
    }

    /**
     * Get the date formatter for parsing dates from filenames.
     *
     * @return the date formatter
     */
    public DateTimeFormatter dateFormatter() {
        String formatStr = getString(COMPLETION_SCHEDULE_DATE_FORMAT_CONFIG);
        try {
            return DateTimeFormatter.ofPattern(formatStr);
        } catch (IllegalArgumentException e) {
            throw new ConfigException(
                COMPLETION_SCHEDULE_DATE_FORMAT_CONFIG,
                formatStr,
                "Invalid date format pattern: " + e.getMessage()
            );
        }
    }

    /**
     * Define the configuration.
     *
     * @return the configuration definition
     */
    public static ConfigDef configDef() {
        return new ConfigDef()
            .define(
                COMPLETION_SCHEDULE_TIME_CONFIG,
                ConfigDef.Type.STRING,
                COMPLETION_SCHEDULE_TIME_DEFAULT,
                new TimeValidator(),
                ConfigDef.Importance.HIGH,
                COMPLETION_SCHEDULE_TIME_DOC
            )
            .define(
                COMPLETION_SCHEDULE_DATE_PATTERN_CONFIG,
                ConfigDef.Type.STRING,
                COMPLETION_SCHEDULE_DATE_PATTERN_DEFAULT,
                new RegexValidator(),
                ConfigDef.Importance.HIGH,
                COMPLETION_SCHEDULE_DATE_PATTERN_DOC
            )
            .define(
                COMPLETION_SCHEDULE_DATE_FORMAT_CONFIG,
                ConfigDef.Type.STRING,
                COMPLETION_SCHEDULE_DATE_FORMAT_DEFAULT,
                new DateFormatValidator(),
                ConfigDef.Importance.HIGH,
                COMPLETION_SCHEDULE_DATE_FORMAT_DOC
            );
    }

    /**
     * Validator for time format (HH:mm:ss).
     */
    private static class TimeValidator implements ConfigDef.Validator {
        @Override
        public void ensureValid(String name, Object value) {
            if (value == null) {
                throw new ConfigException(name, value, "Time configuration is required");
            }
            String timeStr = value.toString();
            try {
                LocalTime.parse(timeStr, DateTimeFormatter.ofPattern("HH:mm:ss"));
            } catch (DateTimeParseException e) {
                throw new ConfigException(
                    name,
                    value,
                    "Invalid time format. Expected format: HH:mm:ss (e.g., '23:59:59')"
                );
            }
        }
    }

    /**
     * Validator for regex pattern.
     */
    private static class RegexValidator implements ConfigDef.Validator {
        @Override
        public void ensureValid(String name, Object value) {
            if (value == null) {
                return; // Has default value
            }
            String patternStr = value.toString();
            try {
                Pattern.compile(patternStr);
            } catch (PatternSyntaxException e) {
                throw new ConfigException(
                    name,
                    value,
                    "Invalid regex pattern: " + e.getMessage()
                );
            }
        }
    }

    /**
     * Validator for date format pattern.
     */
    private static class DateFormatValidator implements ConfigDef.Validator {
        @Override
        public void ensureValid(String name, Object value) {
            if (value == null) {
                return; // Has default value
            }
            String formatStr = value.toString();
            try {
                DateTimeFormatter.ofPattern(formatStr);
            } catch (IllegalArgumentException e) {
                throw new ConfigException(
                    name,
                    value,
                    "Invalid date format pattern. Use a valid DateTimeFormatter pattern (e.g., 'yyyy-MM-dd')"
                );
            }
        }
    }
}