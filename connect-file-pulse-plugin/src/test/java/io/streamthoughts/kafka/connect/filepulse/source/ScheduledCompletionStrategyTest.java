/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.source;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAdjusters;
import java.util.HashMap;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;

/**
 * Test class for {@link ScheduledCompletionStrategy}.
 *
 * <p>Covers all three {@link CompletionPeriod} values (DAILY, WEEKLY, MONTHLY) as well as
 * the {@link LongLivedFileReadStrategy} behaviour inherited by the strategy.
 */
public class ScheduledCompletionStrategyTest {

    private static final DateTimeFormatter MMM_FORMATTER = DateTimeFormatter.ofPattern("MMM");

    private ScheduledCompletionStrategy strategy;

    @Before
    public void setUp() {
        strategy = new ScheduledCompletionStrategy();
    }

    private Map<String, Object> config(String period, String time, String pattern, String format) {
        Map<String, Object> config = new HashMap<>();
        config.put("completion.schedule.period", period);
        config.put("completion.schedule.time", time);
        if (pattern != null) config.put("completion.schedule.date.pattern", pattern);
        if (format  != null) config.put("completion.schedule.date.format",  format);
        return config;
    }

    private FileObjectContext context(String filename) {
        long ts = 1705334400000L; // 2024-01-15 12:00:00 UTC — fixed timestamp, irrelevant for date extraction
        GenericFileObjectMeta meta = new GenericFileObjectMeta.Builder()
            .withUri(URI.create("file:///data/" + filename))
            .withName(filename)
            .withContentLength(1000L)
            .withLastModified(ts)
            .build();
        return new FileObjectContext(meta);
    }

    private String dailyFilename(LocalDate date) {
        return "logs-" + date.format(DateTimeFormatter.ofPattern("yyyy-MM-dd")) + ".log";
    }

    // -------------------------------------------------------------------------
    // DAILY
    // -------------------------------------------------------------------------

    @Test
    public void daily_shouldComplete_forOldFile() {
        strategy.configure(config("DAILY", "01:00:00", null, null));

        FileObjectContext ctx = context(dailyFilename(LocalDate.now().minusDays(10)));
        assertTrue(strategy.shouldComplete(ctx));
    }

    @Test
    public void daily_shouldNotComplete_forTodayFile() {
        strategy.configure(config("DAILY", "01:00:00", null, null));

        FileObjectContext ctx = context(dailyFilename(LocalDate.now()));
        assertFalse(strategy.shouldComplete(ctx));
    }


    @Test
    public void daily_shouldNotComplete_whenDateCannotBeExtracted() {
        strategy.configure(config("DAILY", "01:00:00", null, null));

        assertFalse(strategy.shouldComplete(context("no-date-here.log")));
    }

    @Test
    public void daily_shouldComplete_withCompactDateFormat() {
        strategy.configure(config("DAILY", "01:00:00", ".*?(\\d{8}).*", "yyyyMMdd"));

        LocalDate tenDaysAgo = LocalDate.now().minusDays(10);
        String filename = "app-" + tenDaysAgo.format(DateTimeFormatter.ofPattern("yyyyMMdd")) + ".log";
        assertTrue(strategy.shouldComplete(context(filename)));
    }

    // -------------------------------------------------------------------------
    // WEEKLY — full date format (yyyy-MM-dd)
    // -------------------------------------------------------------------------

    @Test
    public void weekly_shouldComplete_forFileFromTwoWeeksAgo() {
        strategy.configure(config("WEEKLY", "00:01:00", null, null));

        FileObjectContext ctx = context(dailyFilename(LocalDate.now().minusWeeks(2)));
        assertTrue(strategy.shouldComplete(ctx));
    }

    @Test
    public void weekly_shouldNotComplete_forCurrentWeekFile() {
        strategy.configure(config("WEEKLY", "01:00:00", null, null));

        // Use the Monday of the current week — always within the current week regardless of today's day
        LocalDate thisMonday = LocalDate.now().with(TemporalAdjusters.previousOrSame(DayOfWeek.MONDAY));
        FileObjectContext ctx = context(dailyFilename(thisMonday));
        assertFalse(strategy.shouldComplete(ctx));
    }

    // -------------------------------------------------------------------------
    // WEEKLY — week number format (YYYY-'W'ww)
    // -------------------------------------------------------------------------

    @Test
    public void weekly_shouldComplete_forOldWeekNumberFile() {
        // YYYY (week-based year) must be paired with ww (localized week-of-week-based-year).
        // yyyy (calendar year) is incompatible with ww — they live in different resolver chains.
        strategy.configure(config("WEEKLY", "06:00:00", ".*?(\\d{4}-W\\d{2}).*", "YYYY-'W'ww"));

        FileObjectContext ctx = context("report-2024-W01.log");
        assertTrue(strategy.shouldComplete(ctx));
    }


    // -------------------------------------------------------------------------
    // WEEKLY — week-only format (ww), year defaulted to current week-based year
    // Filename convention: report-W<n>.log (e.g. report-W13.log or report-W5.log)
    // -------------------------------------------------------------------------

    private static final String WEEK_ONLY_PATTERN = ".*-W(\\d{1,2})\\.log";
    private static final String WEEK_ONLY_FORMAT  = "ww";

    private String weekOnlyFilename(final int weekNumber) {
        return String.format("report-W%d.log", weekNumber);
    }

    private int currentWeekNumber() {
        return LocalDate.now().get(java.time.temporal.IsoFields.WEEK_OF_WEEK_BASED_YEAR);
    }

    @Test
    public void weekly_weekOnlyFormat_shouldComplete_forOldWeek() {
        strategy.configure(config("WEEKLY", "01:00:00", WEEK_ONLY_PATTERN, WEEK_ONLY_FORMAT));

        int pastWeek = LocalDate.now().minusWeeks(4).get(java.time.temporal.IsoFields.WEEK_OF_WEEK_BASED_YEAR);
        assertTrue(strategy.shouldComplete(context(weekOnlyFilename(pastWeek))));
    }

    @Test
    public void weekly_weekOnlyFormat_shouldComplete_forSingleDigitWeek() {
        // Tests that a 1-digit week number (e.g. report-W1.log) is parsed correctly
        org.junit.Assume.assumeTrue("Requires current week > 2", currentWeekNumber() > 2);
        strategy.configure(config("WEEKLY", "01:00:00", WEEK_ONLY_PATTERN, WEEK_ONLY_FORMAT));

        assertTrue(strategy.shouldComplete(context(weekOnlyFilename(1))));
    }

    @Test
    public void weekly_weekOnlyFormat_shouldNotComplete_forCurrentWeek() {
        strategy.configure(config("WEEKLY", "01:00:00", WEEK_ONLY_PATTERN, WEEK_ONLY_FORMAT));

        assertFalse(strategy.shouldComplete(context(weekOnlyFilename(currentWeekNumber()))));
    }


    // -------------------------------------------------------------------------
    // WEEKLY — configurable week start day
    // -------------------------------------------------------------------------

    @Test
    public void weekly_sundayStart_shouldComplete_forOldWeek() {
        Map<String, Object> cfg = config("WEEKLY", "01:00:00", null, null);
        cfg.put("completion.schedule.week.start.day", "SUNDAY");
        strategy.configure(cfg);

        assertTrue(strategy.shouldComplete(context(dailyFilename(LocalDate.now().minusWeeks(4)))));
    }

    @Test
    public void weekly_sundayStart_shouldNotComplete_forCurrentWeek() {
        Map<String, Object> cfg = config("WEEKLY", "01:00:00", null, null);
        cfg.put("completion.schedule.week.start.day", "SUNDAY");
        strategy.configure(cfg);

        // Use the Sunday of the current week (Sunday-based) — always within bounds regardless of today's day
        LocalDate thisSunday = LocalDate.now().with(TemporalAdjusters.previousOrSame(DayOfWeek.SUNDAY));
        assertFalse(strategy.shouldComplete(context(dailyFilename(thisSunday))));
    }

    // -------------------------------------------------------------------------
    // MONTHLY — full date format (yyyy-MM-dd)
    // -------------------------------------------------------------------------

    @Test
    public void monthly_shouldComplete_forFileFromThreeMonthsAgo() {
        strategy.configure(config("MONTHLY", "01:00:00", null, null));

        FileObjectContext ctx = context(dailyFilename(LocalDate.now().minusMonths(3)));
        assertTrue(strategy.shouldComplete(ctx));
    }

    @Test
    public void monthly_shouldNotComplete_forFileFromThisMonth() {
        strategy.configure(config("MONTHLY", "01:00:00", null, null));

        FileObjectContext ctx = context(dailyFilename(LocalDate.now()));
        assertFalse(strategy.shouldComplete(ctx));
    }

    // -------------------------------------------------------------------------
    // MONTHLY — year-month format (yyyy-MM)
    // -------------------------------------------------------------------------

    @Test
    public void monthly_shouldComplete_forOldYearMonthFile() {
        strategy.configure(config("MONTHLY", "02:00:00", ".*?(\\d{4}-\\d{2}).*", "yyyy-MM"));

        LocalDate threeMonthsAgo = LocalDate.now().minusMonths(3);
        String filename = String.format("export-%d-%02d.log",
            threeMonthsAgo.getYear(), threeMonthsAgo.getMonthValue());
        assertTrue(strategy.shouldComplete(context(filename)));
    }

    @Test
    public void monthly_shouldNotComplete_forCurrentMonthYearMonthFile() {
        strategy.configure(config("MONTHLY", "02:00:00", ".*?(\\d{4}-\\d{2}).*", "yyyy-MM"));

        LocalDate now = LocalDate.now();
        String filename = String.format("export-%d-%02d.log", now.getYear(), now.getMonthValue());
        assertFalse(strategy.shouldComplete(context(filename)));
    }

    // -------------------------------------------------------------------------
    // MONTHLY — month-only format (MM / MMM), year defaulted to current year
    // Covers legacy filename conventions like report-01.csv or report-Jan.csv
    // -------------------------------------------------------------------------

    @Test
    public void monthly_monthOnlyFormat_numeric_shouldComplete_forPastMonth() {
        strategy.configure(config("MONTHLY", "02:00:00", ".*?-(\\d{2})\\.csv", "MM"));

        LocalDate twoMonthsAgo = LocalDate.now().minusMonths(2);
        String filename = String.format("report-%02d.csv", twoMonthsAgo.getMonthValue());
        assertTrue(strategy.shouldComplete(context(filename)));
    }

    @Test
    public void monthly_monthOnlyFormat_numeric_shouldNotComplete_forCurrentMonth() {
        strategy.configure(config("MONTHLY", "02:00:00", ".*?-(\\d{2})\\.csv", "MM"));

        String filename = String.format("report-%02d.csv", LocalDate.now().getMonthValue());
        assertFalse(strategy.shouldComplete(context(filename)));
    }

    @Test
    public void monthly_monthOnlyFormat_text_shouldComplete_forPastMonth() {
        strategy.configure(config("MONTHLY", "02:00:00", ".*?-([A-Za-z]{3})\\.csv", "MMM"));

        LocalDate twoMonthsAgo = LocalDate.now().minusMonths(2);
        String filename = "report-" + twoMonthsAgo.format(MMM_FORMATTER) + ".csv";
        assertTrue(strategy.shouldComplete(context(filename)));
    }

    @Test
    public void monthly_monthOnlyFormat_text_shouldNotComplete_forCurrentMonth() {
        strategy.configure(config("MONTHLY", "02:00:00", ".*?-([A-Za-z]{3})\\.csv", "MMM"));

        String filename = "report-" + LocalDate.now().format(MMM_FORMATTER) + ".csv";
        assertFalse(strategy.shouldComplete(context(filename)));
    }

    // -------------------------------------------------------------------------
    // LongLivedFileReadStrategy
    // -------------------------------------------------------------------------

    @Test
    public void shouldAttemptRead_whenFileIsComplete() {
        strategy.configure(config("DAILY", "01:00:00", null, null));

        long ts = 1705334400000L;
        String filename = dailyFilename(LocalDate.now().minusDays(10));
        GenericFileObjectMeta meta = new GenericFileObjectMeta.Builder()
            .withUri(URI.create("file:///data/" + filename))
            .withName(filename)
            .withContentLength(1000L)
            .withLastModified(ts)
            .build();

        assertTrue(strategy.shouldAttemptRead(meta, new FileObjectOffset(0, 0, ts)));
    }

    @Test
    public void shouldAttemptRead_whenFileIsModifiedButNotComplete() {
        strategy.configure(config("DAILY", "23:59:59", null, null));

        long fileModified = 1705334400000L; // 12:00 UTC
        long offsetTime   = 1705330800000L; // 11:00 UTC — older than file
        String filename = dailyFilename(LocalDate.now().plusDays(5));
        GenericFileObjectMeta meta = new GenericFileObjectMeta.Builder()
            .withUri(URI.create("file:///data/" + filename))
            .withName(filename)
            .withContentLength(1000L)
            .withLastModified(fileModified)
            .build();

        assertTrue(strategy.shouldAttemptRead(meta, new FileObjectOffset(0, 0, offsetTime)));
    }

    @Test
    public void shouldNotAttemptRead_whenFileIsNotModifiedAndNotComplete() {
        strategy.configure(config("DAILY", "23:59:59", null, null));

        long fileModified = 1705330800000L; // 11:00 UTC
        long offsetTime   = 1705334400000L; // 12:00 UTC — newer than file

        String filename = dailyFilename(LocalDate.now());
        GenericFileObjectMeta meta = new GenericFileObjectMeta.Builder()
            .withUri(URI.create("file:///data/" + filename))
            .withName(filename)
            .withContentLength(1000L)
            .withLastModified(fileModified)
            .build();

        assertFalse(strategy.shouldAttemptRead(meta, new FileObjectOffset(0, 0, offsetTime)));
    }
}
