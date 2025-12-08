/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.source;

import io.streamthoughts.kafka.connect.filepulse.config.DailyCompletionStrategyConfig;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link FileCompletionStrategy} that marks files as COMPLETED
 * at a scheduled time by extracting the date from the filename.
 *
 * <p>This is useful for "daily" files that are continuously appended throughout the day
 * and should only be marked as complete the <strong>next day</strong> at a specific time.
 *
 * <p>The strategy extracts the date from the filename using a regex pattern. The file is completed
 * on the <strong>day after</strong> the date in the filename at the configured completion time.
 * Uses the system default timezone.
 *
 * <h3>Example</h3>
 * <ul>
 *   <li>File: <code>logs-2025-12-08.log</code></li>
 *   <li>Completion time: <code>01:00:00</code></li>
 *   <li>File created: December 8, 2025 at 6:00 AM</li>
 *   <li><strong>File will be completed: December 9, 2025 at 01:00:00</strong></li>
 * </ul>
 *
 * <p>This ensures that all data written during the day (December 8) is collected,
 * with a 1-hour buffer into the next day before the file is marked complete.
 *
 * <h3>Configuration</h3>
 * <ul>
 *   <li><code>completion.schedule.time</code>: Time to complete files (HH:mm:ss format, e.g., "01:00:00")</li>
 *   <li><code>completion.schedule.date.pattern</code>:
 *      Regex pattern to extract date from filename (default: ".*?(\\d{4}-\\d{2}-\\d{2}).*")</li>
 *   <li><code>completion.schedule.date.format</code>: Date format in the filename (default: "yyyy-MM-dd")</li>
 * </ul>
 *
 * <h3>Example filename patterns</h3>
 * <ul>
 *   <li><code>logs-2025-12-08.log</code> → pattern: ".*?(\\d{4}-\\d{2}-\\d{2}).*", format: "yyyy-MM-dd"</li>
 *   <li><code>app-20251208.log</code> → pattern: ".*?(\\d{8}).*", format: "yyyyMMdd"</li>
 * </ul>
 */
public class DailyCompletionStrategy implements FileCompletionStrategy, LongLivedFileReadStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(DailyCompletionStrategy.class);

    private LocalTime scheduledCompletionTime;
    private Pattern datePattern;
    private DateTimeFormatter dateFormatter;

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(final Map<String, ?> configs) {
        DailyCompletionStrategyConfig config = new DailyCompletionStrategyConfig(configs);
        this.scheduledCompletionTime = config.scheduledCompletionTime();
        this.datePattern = config.datePattern();
        this.dateFormatter = config.dateFormatter();

        LOG.info(
            "Configured DailyCompletionStrategy: completionTime={}, datePattern={}, dateFormat={}, timezone={}",
            scheduledCompletionTime, datePattern.pattern(), dateFormatter, ZoneId.systemDefault()
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean shouldComplete(final FileObjectContext context) {
        // Extract date from filename
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
            // If we can't extract the date, don't complete the file
            return false;
        }

        // Calculate when this file should be completed
        Instant fileCompletionTime = calculateCompletionTimeForDate(fileDate);
        Instant now = Instant.now();

        boolean timeReached = now.isAfter(fileCompletionTime) || now.equals(fileCompletionTime);

        if (timeReached) {
            LOG.info(
                "Scheduled completion time reached for file '{}' (date: {}, completion time: {}, now: {})",
                context.metadata().stringURI(),
                fileDate,
                fileCompletionTime,
                now
            );
        }

        return timeReached;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean shouldAttemptRead(final FileObjectMeta objectMeta, final FileObjectOffset offset) {
        // Always attempt to read if the file should be completed
        boolean shouldRead = shouldComplete(new FileObjectContext(objectMeta))
            || LongLivedFileReadStrategy.super.shouldAttemptRead(objectMeta, offset);

        if (!shouldRead) {
            LOG.debug("Deferring read for file '{}' until file is updated or scheduled completion time is reached.",
                objectMeta.stringURI());
        }

        return shouldRead;
    }

    /**
     * Extract the date from the filename using the configured pattern and format.
     *
     * @param filename the filename or URI
     * @return the parsed date
     * @throws IllegalArgumentException if the date cannot be extracted or parsed
     */
    private LocalDate extractDateFromFilename(String filename) {
        Matcher matcher = datePattern.matcher(filename);
        if (!matcher.matches()) {
            throw new IllegalArgumentException(
                "Filename does not match date pattern: " + filename
            );
        }

        // Extract the date string from the first capturing group
        String dateStr = matcher.group(1);

        try {
            return LocalDate.parse(dateStr, dateFormatter);
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException(
                "Could not parse date '" + dateStr + "' from filename '" +
                    filename + "' using format: " + dateFormatter,
                e
            );
        }
    }

    /**
     * Calculate the completion time for a given file date using the system default timezone.
     * The completion time is the configured time on the <strong>day after</strong> the file's date.
     *
     * <p>For example, if the file is for 2025-12-08 and completion time is 01:00:00,
     * the file should be completed at <strong>2025-12-09 01:00:00</strong> (in the system timezone).
     *
     * <p>This ensures that all data written during the file's day (Dec 8) is collected,
     * with a buffer period into the next day before marking the file as complete.
     *
     * @param fileDate the date extracted from the filename
     * @return the instant when the file should be completed
     */
    private Instant calculateCompletionTimeForDate(LocalDate fileDate) {
        LocalDate completionDate = fileDate.plusDays(1);
        LocalDateTime completionDateTime = LocalDateTime.of(completionDate, scheduledCompletionTime);
        return completionDateTime.atZone(ZoneId.systemDefault()).toInstant();
    }
}