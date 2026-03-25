/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.source;

import io.streamthoughts.kafka.connect.filepulse.config.ScheduledCompletionStrategyConfig;
import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.time.temporal.IsoFields;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A generic {@link FileCompletionStrategy} that marks long-lived files as COMPLETED
 * at a configurable scheduled time relative to the date extracted from the filename.
 *
 * <p>This is useful for files that are continuously appended over a period (day, week, month)
 * and should only be marked as complete once that period has ended, plus a configurable time buffer.
 * The strategy also implements {@link LongLivedFileReadStrategy} to avoid unnecessary polling
 * when no new data is expected.
 *
 * <h3>How completion is calculated</h3>
 * <ol>
 *   <li>The date is extracted from the filename using the configured regex pattern.</li>
 *   <li>The next period boundary is computed from that date according to
 *       the configured {@link CompletionPeriod}.</li>
 *   <li>The file is marked COMPLETED when the current time is at or after
 *       {@code <nextPeriodBoundary> + <scheduledTime>} in the system default timezone.</li>
 * </ol>
 *
 * <h3>Examples</h3>
 * <table border="1">
 *   <tr><th>Period</th><th>File date</th><th>Scheduled time</th><th>Completes at</th></tr>
 *   <tr><td>DAILY</td>  <td>2026-03-10</td><td>01:00:00</td><td>2026-03-11 01:00:00</td></tr>
 *   <tr><td>WEEKLY</td> <td>2026-03-10 (Tue)</td><td>01:00:00</td><td>2026-03-16 (Mon) 01:00:00</td></tr>
 *   <tr><td>MONTHLY</td><td>2026-03-10</td><td>01:00:00</td><td>2026-04-01 01:00:00</td></tr>
 * </table>
 *
 * <h3>Configuration</h3>
 * <ul>
 *   <li>{@code completion.schedule.time}         – Time-of-day (HH:mm:ss) at which to complete
 *       (default: {@code 00:01:00}).</li>
 *   <li>{@code completion.schedule.date.pattern} – Regex with one capturing group for the date in the filename
 *       (default: {@code .*?(\d{4}-\d{2}-\d{2}).*}).</li>
 *   <li>{@code completion.schedule.date.format}  – DateTimeFormatter pattern for the captured date
 *       (default: {@code yyyy-MM-dd}).</li>
 *   <li>{@code completion.schedule.period}       – {@link CompletionPeriod}: {@code DAILY} (default),
 *       {@code WEEKLY}, or {@code MONTHLY}.</li>
 * </ul>
 */
public class ScheduledCompletionStrategy implements FileCompletionStrategy, LongLivedFileReadStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(ScheduledCompletionStrategy.class);

    private LocalTime scheduledCompletionTime;
    private Pattern datePattern;
    private DateTimeFormatter dateFormatter;
    private CompletionPeriod completionPeriod;
    private DayOfWeek weekStartDay;
    private boolean requiresWeekBasedYearDefault;
    private boolean requiresYearDefault;

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(final Map<String, ?> configs) {
        ScheduledCompletionStrategyConfig config = new ScheduledCompletionStrategyConfig(configs);
        this.scheduledCompletionTime = config.scheduledCompletionTime();
        this.datePattern = config.datePattern();
        this.dateFormatter = config.dateFormatter();
        this.completionPeriod = config.completionPeriod();
        this.weekStartDay = config.weekStartDay();
        this.requiresWeekBasedYearDefault = config.requiresWeekBasedYearDefault();
        this.requiresYearDefault = config.requiresYearDefault();

        LOG.info(
            "Configured ScheduledCompletionStrategy: period={}, completionTime={}, " +
                "datePattern={}, dateFormat={}, timezone={}",
            completionPeriod,
            scheduledCompletionTime,
            datePattern.pattern(),
            dateFormatter,
            ZoneId.systemDefault()
        );
    }

    /**
     * {@inheritDoc}
     *
     * <p>Returns {@code true} when the current instant is at or after the completion instant
     * computed from the date in the filename and the configured period and time.
     */
    @Override
    public boolean shouldComplete(final FileObjectContext context) {
        LocalDate fileDate;
        try {
            fileDate = extractDateFromFilename(context.metadata().stringURI());
        } catch (Exception e) {
            LOG.warn(
                "Could not extract date from filename '{}': {}. " +
                    "File will not be completed until date can be determined.",
                context.metadata().stringURI(),
                e.getMessage()
            );
            return false;
        }

        Instant fileCompletionInstant = calculateCompletionInstant(fileDate);
        Instant now = Instant.now();
        boolean shouldComplete = !now.isBefore(fileCompletionInstant) || fileDate.isAfter(LocalDate.now());

        if (shouldComplete) {
            LOG.info(
                "Scheduled completion time reached for file '{}' " +
                    "(period={}, fileDate={}, completionInstant={}, now={})",
                context.metadata().stringURI(),
                completionPeriod,
                fileDate,
                fileCompletionInstant,
                now
            );
        }

        return shouldComplete;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Attempts a read either when the scheduled completion time has been reached
     * (so the file can be flushed) or when the file has been modified since the last
     * known offset (default {@link LongLivedFileReadStrategy} behaviour).
     */
    @Override
    public boolean shouldAttemptRead(final FileObjectMeta objectMeta, final FileObjectOffset offset) {
        boolean shouldRead = shouldComplete(new FileObjectContext(objectMeta))
            || LongLivedFileReadStrategy.super.shouldAttemptRead(objectMeta, offset);

        if (!shouldRead) {
            LOG.debug(
                "Deferring read for file '{}' until file is updated or scheduled completion time is reached.",
                objectMeta.stringURI()
            );
        }

        return shouldRead;
    }

    /**
     * Extracts and parses the date from the given filename (or URI string).
     *
     * <p>The date string is captured from the filename using the configured regex pattern.
     * For formats that include a year field, the base {@link DateTimeFormatter} built at configure time
     * is used directly. For partial formats without a year field, a fresh formatter is built at each
     * call by prepending a {@code parseDefaulting} for the current year (via {@link LocalDate#now()}),
     * ensuring correctness across year boundaries without a connector restart:
     *
     * @param filename the filename or URI string.
     * @return the parsed {@link LocalDate}.
     * @throws IllegalArgumentException if the filename does not match the pattern or the date cannot be parsed.
     */
    private LocalDate extractDateFromFilename(final String filename) {
        Matcher matcher = datePattern.matcher(filename);
        if (!matcher.matches()) {
            throw new IllegalArgumentException(
                "Filename does not match date pattern '" + datePattern.pattern() + "': " + filename
            );
        }

        String dateStr = matcher.group(1);

        try {
            DateTimeFormatter formatter = resolveFormatter();
            return LocalDate.from(formatter.parse(dateStr));
        } catch (Exception e) {
            throw new IllegalArgumentException(
                "Could not parse date '" + dateStr + "' from filename '" +
                    filename + "' using format '" + dateFormatter + "'",
                e
            );
        }
    }

    /**
     * Returns the formatter to use for parsing the date string captured from the filename.
     *
     * <p>When a year default is required (week-only or month-only format), a fresh formatter is built
     * at each call using {@link LocalDate#now()} so the year is always current
     *
     * @return the formatter to use.
     */
    private DateTimeFormatter resolveFormatter() {
        if (requiresWeekBasedYearDefault) {
            return new DateTimeFormatterBuilder()
                .parseDefaulting(IsoFields.WEEK_BASED_YEAR, LocalDate.now().get(IsoFields.WEEK_BASED_YEAR))
                .parseDefaulting(ChronoField.DAY_OF_WEEK, weekStartDay.getValue())
                .appendValue(IsoFields.WEEK_OF_WEEK_BASED_YEAR)
                .toFormatter();
        }
        if (requiresYearDefault) {
            return new DateTimeFormatterBuilder()
                .parseDefaulting(ChronoField.YEAR, LocalDate.now().getYear())
                .append(dateFormatter)
                .toFormatter();
        }
        return dateFormatter;
    }

    /**
     * Calculates the {@link Instant} at which the file should be completed.
     *
     * <p>The completion instant is:
     * {@code nextPeriodBoundary(fileDate) @ scheduledCompletionTime} in the system default timezone.
     *
     * @param fileDate the date extracted from the filename.
     * @return the instant when the file should be completed.
     */
    private Instant calculateCompletionInstant(final LocalDate fileDate) {
        LocalDate completionDate = completionPeriod.nextCompletionDate(fileDate, weekStartDay);
        LocalDateTime completionDateTime = LocalDateTime.of(completionDate, scheduledCompletionTime);
        return completionDateTime.atZone(ZoneId.systemDefault()).toInstant();
    }
}
