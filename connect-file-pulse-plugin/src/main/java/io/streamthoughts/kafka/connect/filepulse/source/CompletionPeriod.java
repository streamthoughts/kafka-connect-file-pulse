/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.source;

import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.temporal.TemporalAdjusters;

/**
 * Defines the period unit used by {@link ScheduledCompletionStrategy} to determine
 * when a file should be marked as COMPLETED relative to the date extracted from its filename.
 *
 * <p>The date format configured via
 * {@link io.streamthoughts.kafka.connect.filepulse.config.ScheduledCompletionStrategyConfig#dateFormatter()}
 * may be partial or full depending on the period:
 * <ul>
 *   <li>{@link #DAILY}   – expects a full date (e.g. {@code yyyy-MM-dd})</li>
 *   <li>{@link #WEEKLY}  – supports week-based formats (e.g. {@code YYYY-'W'ww}) or full date formats
 *       (e.g. {@code yyyy-MM-dd})</li>
 *   <li>{@link #MONTHLY} – supports year-month formats (e.g. {@code yyyy-MM}) or full date formats
 *       (e.g. {@code yyyy-MM-dd})</li>
 * </ul>
 */
public enum CompletionPeriod {

    /**
     * Complete on the day immediately following the file's date.
     */
    DAILY {
        @Override
        public LocalDate nextCompletionDate(final LocalDate fileDate, final DayOfWeek weekStartDay) {
            return fileDate.plusDays(1);
        }
    },

    /**
     * Complete on the first day of the next week (as defined by {@code weekStartDay}) following
     * the week that contains the file's date.
     */
    WEEKLY {
        @Override
        public LocalDate nextCompletionDate(final LocalDate fileDate, final DayOfWeek weekStartDay) {
            LocalDate thisWeekStart = fileDate.with(TemporalAdjusters.previousOrSame(weekStartDay));
            return thisWeekStart.plusWeeks(1);
        }
    },

    /**
     * Complete on the first day of the month immediately following the file's date's month.
     */
    MONTHLY {
        @Override
        public LocalDate nextCompletionDate(final LocalDate fileDate, final DayOfWeek weekStartDay) {
            return fileDate.with(TemporalAdjusters.firstDayOfNextMonth());
        }
    };

    /**
     * Calculates the date on which the file should be marked as completed,
     * based on the date extracted from the filename.
     *
     * @param fileDate     the date extracted from the filename.
     * @param weekStartDay the first day of the week, used by {@link #WEEKLY} to compute the next
     *                     week boundary. Ignored by {@link #DAILY} and {@link #MONTHLY}.
     * @return the date on which the file should be completed (at the configured scheduled time).
     */
    public abstract LocalDate nextCompletionDate(LocalDate fileDate, DayOfWeek weekStartDay);

    /**
     * Convenience overload using {@link DayOfWeek#MONDAY} as the week start day.
     * Suitable for {@link #DAILY} and {@link #MONTHLY} where the week start day is irrelevant.
     *
     * @param fileDate the date extracted from the filename.
     * @return the date on which the file should be completed.
     */
    public LocalDate nextCompletionDate(final LocalDate fileDate) {
        return nextCompletionDate(fileDate, DayOfWeek.MONDAY);
    }
}
