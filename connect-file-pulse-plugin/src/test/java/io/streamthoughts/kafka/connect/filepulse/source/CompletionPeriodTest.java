/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.source;

import static org.junit.Assert.assertEquals;

import java.time.DayOfWeek;
import java.time.LocalDate;
import org.junit.Test;

/**
 * Test class for {@link CompletionPeriod}.
 */
public class CompletionPeriodTest {

    @Test
    public void daily_nextCompletionDate_shouldReturnNextDay() {
        LocalDate date = LocalDate.of(2026, 3, 10);
        assertEquals(LocalDate.of(2026, 3, 11), CompletionPeriod.DAILY.nextCompletionDate(date));
    }

    @Test
    public void weekly_nextCompletionDate_fromTuesday_withMondayStart_shouldReturnFollowingMonday() {
        // Tuesday 2026-03-10 → next week's Monday = 2026-03-16
        LocalDate tuesday = LocalDate.of(2026, 3, 10);
        assertEquals(LocalDate.of(2026, 3, 16),
            CompletionPeriod.WEEKLY.nextCompletionDate(tuesday, DayOfWeek.MONDAY));
    }

    @Test
    public void weekly_nextCompletionDate_fromMonday_withMondayStart_shouldReturnFollowingMonday() {
        // Monday 2026-03-09 → next week's Monday = 2026-03-16
        LocalDate monday = LocalDate.of(2026, 3, 9);
        assertEquals(LocalDate.of(2026, 3, 16),
            CompletionPeriod.WEEKLY.nextCompletionDate(monday, DayOfWeek.MONDAY));
    }

    @Test
    public void weekly_nextCompletionDate_fromTuesday_withSundayStart_shouldReturnFollowingSunday() {
        // Tuesday 2026-03-10, SUNDAY week start: this week's Sunday was 2026-03-08 → next = 2026-03-15
        LocalDate tuesday = LocalDate.of(2026, 3, 10);
        assertEquals(LocalDate.of(2026, 3, 15),
            CompletionPeriod.WEEKLY.nextCompletionDate(tuesday, DayOfWeek.SUNDAY));
    }

    @Test
    public void monthly_nextCompletionDate_shouldReturnFirstDayOfNextMonth() {
        LocalDate date = LocalDate.of(2026, 3, 10);
        assertEquals(LocalDate.of(2026, 4, 1), CompletionPeriod.MONTHLY.nextCompletionDate(date));
    }

    @Test
    public void monthly_nextCompletionDate_fromLastDayOfMonth_shouldReturnFirstDayOfNextMonth() {
        LocalDate lastDay = LocalDate.of(2026, 3, 31);
        assertEquals(LocalDate.of(2026, 4, 1), CompletionPeriod.MONTHLY.nextCompletionDate(lastDay));
    }
}
