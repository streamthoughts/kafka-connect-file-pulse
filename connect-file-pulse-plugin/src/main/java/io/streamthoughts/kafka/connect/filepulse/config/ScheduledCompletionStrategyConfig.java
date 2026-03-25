/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.config;

import io.streamthoughts.kafka.connect.filepulse.source.CompletionPeriod;
import io.streamthoughts.kafka.connect.filepulse.source.ScheduledCompletionStrategy;
import java.time.DayOfWeek;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

/**
 * Configuration for {@link ScheduledCompletionStrategy}.
 *
 * <h3>Configuration keys</h3>
 * <ul>
 *   <li>{@code completion.schedule.time}         – Time-of-day (HH:mm:ss) at which the file is completed.</li>
 *   <li>{@code completion.schedule.date.pattern} – Regex with one capturing group for the date in the filename.</li>
 *   <li>{@code completion.schedule.date.format}  – {@link DateTimeFormatter} pattern for that date.</li>
 *   <li>{@code completion.schedule.period}       – {@link CompletionPeriod}: {@code DAILY} (default),
 *       {@code WEEKLY}, or {@code MONTHLY}.</li>
 *   <li>{@code completion.schedule.week.start.day} – First day of the week for {@code WEEKLY} period
 *       when no day field is present in the format (default: {@code MONDAY}).</li>
 * </ul>
 */
public class ScheduledCompletionStrategyConfig extends AbstractConfig {

    public static final String COMPLETION_SCHEDULE_TIME_CONFIG = "completion.schedule.time";
    private static final String COMPLETION_SCHEDULE_TIME_DOC =
        "Time of day (HH:mm:ss) at which the file is marked as COMPLETED after the period boundary " +
        "computed from the date extracted from the filename. Uses the system default timezone.";
    private static final String COMPLETION_SCHEDULE_TIME_DEFAULT = "00:01:00";

    public static final String COMPLETION_SCHEDULE_DATE_PATTERN_CONFIG = "completion.schedule.date.pattern";
    private static final String COMPLETION_SCHEDULE_DATE_PATTERN_DOC =
        "Regex pattern used to extract the date from the filename. Must contain exactly one capturing group " +
        "that matches the date portion. " +
        "Example: '.*?(\\d{4}-\\d{2}-\\d{2}).*' matches dates like '2025-12-08' in 'logs-2025-12-08.log'.";
    private static final String COMPLETION_SCHEDULE_DATE_PATTERN_DEFAULT = ".*?(\\d{4}-\\d{2}-\\d{2}).*";

    public static final String COMPLETION_SCHEDULE_DATE_FORMAT_CONFIG = "completion.schedule.date.format";
    private static final String COMPLETION_SCHEDULE_DATE_FORMAT_DOC =
        "DateTimeFormatter pattern used to parse the date captured from the filename. " +
        "Must match the format captured by the regex group (e.g. 'yyyy-MM-dd' for '2025-12-08', " +
        "'yyyyMMdd' for '20251208').";
    private static final String COMPLETION_SCHEDULE_DATE_FORMAT_DEFAULT = "yyyy-MM-dd";

    public static final String COMPLETION_SCHEDULE_PERIOD_CONFIG = "completion.schedule.period";
    private static final String COMPLETION_SCHEDULE_PERIOD_DOC =
        "Period unit that controls when the file is completed relative to the date in its filename. " +
        "Accepted values: DAILY (complete the day after), WEEKLY (complete the next configured week start day), " +
        "MONTHLY (complete on the 1st day of the next month). Default: DAILY.";
    private static final String COMPLETION_SCHEDULE_PERIOD_DEFAULT = CompletionPeriod.DAILY.name();

    public static final String COMPLETION_SCHEDULE_WEEK_START_DAY_CONFIG = "completion.schedule.week.start.day";
    private static final String COMPLETION_SCHEDULE_WEEK_START_DAY_DOC =
        "The first day of the week, used when no day field is present in the date format (WEEKLY period only). " +
        "Accepted values: MONDAY, TUESDAY, WEDNESDAY, THURSDAY, FRIDAY, SATURDAY, SUNDAY. Default: MONDAY.";
    private static final String COMPLETION_SCHEDULE_WEEK_START_DAY_DEFAULT = "MONDAY";

    /**
     * Creates a new {@link ScheduledCompletionStrategyConfig} instance.
     *
     * @param originals the configuration properties.
     */
    public ScheduledCompletionStrategyConfig(final Map<?, ?> originals) {
        super(configDef(), originals);
    }

    /**
     * Returns the scheduled completion time of day.
     *
     * @return the parsed {@link LocalTime}.
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
     * Returns the compiled regex {@link Pattern} used to extract the date from filenames.
     *
     * @return the compiled pattern.
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
     * Returns a base {@link DateTimeFormatter} built from the configured pattern, with structural
     * field defaults that do not depend on the current date:
     * <ul>
     *   <li>{@link CompletionPeriod#WEEKLY} – defaults {@code DAY_OF_WEEK} to the configured
     *       {@link #weekStartDay()} if no day field is present.</li>
     *   <li>{@link CompletionPeriod#MONTHLY} – defaults {@code DAY_OF_MONTH} to 1 if no day field
     *       is present.</li>
     * </ul>
     *
     * <p>Date-relative defaults ({@code WEEK_BASED_YEAR}, {@code YEAR}) are intentionally <em>not</em>
     * applied here — they must be injected fresh at each parse call in
     * {@link io.streamthoughts.kafka.connect.filepulse.source.ScheduledCompletionStrategy}
     * so that {@code LocalDate.now()} always reflects the actual current date, even across year
     * boundaries without a connector restart.
     *
     * @return the base date formatter.
     * @throws ConfigException if the pattern is invalid.
     */
    public DateTimeFormatter dateFormatter() {
        String formatStr = getString(COMPLETION_SCHEDULE_DATE_FORMAT_CONFIG);
        CompletionPeriod period = completionPeriod();
        try {
            DateTimeFormatterBuilder builder = new DateTimeFormatterBuilder();

            switch (period) {
                case WEEKLY:
                    // Default DAY_OF_WEEK to the configured week start day if no explicit day field is present
                    if (!formatStr.contains("d") && !formatStr.contains("e") && !formatStr.contains("E")) {
                        builder.parseDefaulting(ChronoField.DAY_OF_WEEK, weekStartDay().getValue());
                    }
                    break;
                case MONTHLY:
                    // Default DAY_OF_MONTH to 1 if no explicit day field is present
                    if (!formatStr.contains("d")) {
                        builder.parseDefaulting(ChronoField.DAY_OF_MONTH, 1);
                    }
                    break;
                default:
                    break;
            }

            return builder.appendPattern(formatStr).toFormatter();
        } catch (IllegalArgumentException e) {
            throw new ConfigException(
                COMPLETION_SCHEDULE_DATE_FORMAT_CONFIG,
                formatStr,
                "Invalid date format pattern: " + e.getMessage()
            );
        }
    }

    /**
     * Returns {@code true} if the configured date format contains no year field ({@code Y} or {@code y})
     * and the period is {@link CompletionPeriod#WEEKLY}.
     *
     * <p>When {@code true}, {@code WEEK_BASED_YEAR} must be injected fresh at each parse call.
     *
     * @return {@code true} if a fresh week-based year default must be applied at parse time.
     */
    public boolean requiresWeekBasedYearDefault() {
        if (completionPeriod() != CompletionPeriod.WEEKLY) return false;
        String formatStr = getString(COMPLETION_SCHEDULE_DATE_FORMAT_CONFIG);
        return !formatStr.contains("Y") && !formatStr.contains("y");
    }

    /**
     * Returns {@code true} if the configured date format contains no year field ({@code Y} or {@code y})
     * and the period is {@link CompletionPeriod#MONTHLY}.
     *
     * <p>When {@code true}, {@code YEAR} must be injected fresh at each parse call.
     *
     * @return {@code true} if a fresh calendar year default must be applied at parse time.
     */
    public boolean requiresYearDefault() {
        if (completionPeriod() != CompletionPeriod.MONTHLY) return false;
        String formatStr = getString(COMPLETION_SCHEDULE_DATE_FORMAT_CONFIG);
        return !formatStr.contains("y") && !formatStr.contains("Y");
    }

    /**
     * Returns the {@link CompletionPeriod} that controls the completion boundary.
     *
     * @return the completion period.
     */
    public CompletionPeriod completionPeriod() {
        String periodStr = getString(COMPLETION_SCHEDULE_PERIOD_CONFIG);
        try {
            return CompletionPeriod.valueOf(periodStr.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new ConfigException(
                COMPLETION_SCHEDULE_PERIOD_CONFIG,
                periodStr,
                "Invalid period. Accepted values: DAILY, WEEKLY, MONTHLY"
            );
        }
    }

    /**
     * Returns the configured first day of the week, used to resolve partial week-based date formats.
     *
     * <p>Only relevant for the {@link CompletionPeriod#WEEKLY} period when no day field is present
     * in the date format (e.g. {@code yyyy-'W'ww} or {@code ww}). Defaults to {@link DayOfWeek#MONDAY}.
     *
     * @return the configured {@link DayOfWeek}.
     */
    public DayOfWeek weekStartDay() {
        String dayStr = getString(COMPLETION_SCHEDULE_WEEK_START_DAY_CONFIG);
        try {
            return DayOfWeek.valueOf(dayStr.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new ConfigException(
                COMPLETION_SCHEDULE_WEEK_START_DAY_CONFIG,
                dayStr,
                "Invalid day. Accepted values: MONDAY, TUESDAY, WEDNESDAY, THURSDAY, FRIDAY, SATURDAY, SUNDAY"
            );
        }
    }

    /**
     * Returns the {@link ConfigDef} for this configuration class.
     *
     * @return the config definition.
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
            )
            .define(
                COMPLETION_SCHEDULE_PERIOD_CONFIG,
                ConfigDef.Type.STRING,
                COMPLETION_SCHEDULE_PERIOD_DEFAULT,
                new PeriodValidator(),
                ConfigDef.Importance.HIGH,
                COMPLETION_SCHEDULE_PERIOD_DOC
            )
            .define(
                COMPLETION_SCHEDULE_WEEK_START_DAY_CONFIG,
                ConfigDef.Type.STRING,
                COMPLETION_SCHEDULE_WEEK_START_DAY_DEFAULT,
                new WeekStartDayValidator(),
                ConfigDef.Importance.LOW,
                COMPLETION_SCHEDULE_WEEK_START_DAY_DOC
            );
    }

    static class TimeValidator implements ConfigDef.Validator {
        @Override
        public void ensureValid(final String name, final Object value) {
            if (value == null) {
                throw new ConfigException(name, null, "Time configuration is required");
            }
            try {
                LocalTime.parse(value.toString(), DateTimeFormatter.ofPattern("HH:mm:ss"));
            } catch (DateTimeParseException e) {
                throw new ConfigException(
                    name, value, "Invalid time format. Expected format: HH:mm:ss (e.g., '23:59:59')"
                );
            }
        }
    }

    static class RegexValidator implements ConfigDef.Validator {
        @Override
        public void ensureValid(final String name, final Object value) {
            if (value == null) return;
            try {
                Pattern.compile(value.toString());
            } catch (PatternSyntaxException e) {
                throw new ConfigException(name, value, "Invalid regex pattern: " + e.getMessage());
            }
        }
    }

    static class DateFormatValidator implements ConfigDef.Validator {
        @Override
        public void ensureValid(final String name, final Object value) {
            if (value == null) return;
            try {
                DateTimeFormatter.ofPattern(value.toString());
            } catch (IllegalArgumentException e) {
                throw new ConfigException(
                    name, value,
                    "Invalid date format pattern. Use a valid DateTimeFormatter pattern (e.g., 'yyyy-MM-dd')"
                );
            }
        }
    }

    static class PeriodValidator implements ConfigDef.Validator {
        @Override
        public void ensureValid(final String name, final Object value) {
            if (value == null) return;
            try {
                CompletionPeriod.valueOf(value.toString().toUpperCase());
            } catch (IllegalArgumentException e) {
                throw new ConfigException(
                    name, value, "Invalid period. Accepted values: DAILY, WEEKLY, MONTHLY"
                );
            }
        }
    }

    static class WeekStartDayValidator implements ConfigDef.Validator {
        @Override
        public void ensureValid(final String name, final Object value) {
            if (value == null) return;
            try {
                DayOfWeek.valueOf(value.toString().toUpperCase());
            } catch (IllegalArgumentException e) {
                throw new ConfigException(
                    name, value,
                    "Invalid day. Accepted values: MONDAY, TUESDAY, WEDNESDAY, THURSDAY, FRIDAY, SATURDAY, SUNDAY"
                );
            }
        }
    }
}
