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
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;

/**
 * Test class for {@link DailyCompletionStrategy}.
 */
public class DailyCompletionStrategyTest {

    private DailyCompletionStrategy strategy;
    private static final String DEFAULT_TIME = "01:00:00";

    @Before
    public void setUp() {
        strategy = new DailyCompletionStrategy();
    }

    /**
     * Helper method to create configuration map.
     */
    private Map<String, Object> createConfig(String time, String pattern, String format) {
        Map<String, Object> config = new HashMap<>();
        config.put("daily.completion.schedule.time", time);
        if (pattern != null) {
            config.put("daily.completion.schedule.date.pattern", pattern);
        }
        if (format != null) {
            config.put("daily.completion.schedule.date.format", format);
        }
        return config;
    }

    /**
     * Helper method to create FileObjectContext with a given filename.
     * Uses a fixed timestamp (2024-01-15 12:00:00 UTC) for last modified time.
     */
    private FileObjectContext createContext(String filename) {
        long defaultTimestamp = 1705334400000L; // 2024-01-15 12:00:00 UTC
        GenericFileObjectMeta meta = new GenericFileObjectMeta.Builder()
                .withUri(URI.create("file:///tmp/" + filename))
                .withName(filename)
                .withContentLength(1000L)
                .withLastModified(defaultTimestamp)
                .build();
        return new FileObjectContext(meta);
    }

    @Test
    public void testShouldCompleteWithStandardFilename() {
        Map<String, Object> config = createConfig(DEFAULT_TIME, null, null);
        strategy.configure(config);

        // File from 10 days ago - should be complete
        LocalDate tenDaysAgo = LocalDate.now().minusDays(10);
        String filename = String.format("logs-%s.log", tenDaysAgo.format(DateTimeFormatter.ofPattern("yyyy-MM-dd")));
        FileObjectContext context = createContext(filename);

        assertTrue("Old file should be marked as complete",
                   strategy.shouldComplete(context));
    }

    @Test
    public void testShouldCompleteWithCompactDateFormat() {
        Map<String, Object> config = createConfig("02:00:00", ".*?(\\d{8}).*", "yyyyMMdd");
        strategy.configure(config);

        // File from 10 days ago in compact format
        LocalDate tenDaysAgo = LocalDate.now().minusDays(10);
        String filename = String.format("app-%s.log", tenDaysAgo.format(DateTimeFormatter.ofPattern("yyyyMMdd")));
        FileObjectContext context = createContext(filename);

        // Old file should be complete
        assertTrue("Old file with compact date should be complete",
                   strategy.shouldComplete(context));
    }

    @Test
    public void testShouldCompleteWithDateInMiddleOfFilename() {
        Map<String, Object> config = createConfig(DEFAULT_TIME, null, null);
        strategy.configure(config);

        // File from 10 days ago with date in middle
        LocalDate tenDaysAgo = LocalDate.now().minusDays(10);
        String filename = String.format("application-%s-server.log", tenDaysAgo.format(DateTimeFormatter.ofPattern("yyyy-MM-dd")));
        FileObjectContext context = createContext(filename);

        assertTrue("Old file with date in middle should be complete",
                   strategy.shouldComplete(context));
    }

    @Test
    public void testShouldNotCompleteWhenDateCannotBeExtracted() {
        Map<String, Object> config = createConfig(DEFAULT_TIME, null, null);
        strategy.configure(config);

        FileObjectContext context = createContext("no-date-in-filename.log");

        assertFalse("Should not complete when date cannot be extracted",
                    strategy.shouldComplete(context));
    }

    @Test
    public void testShouldNotCompleteWhenDateFormatDoesNotMatch() {
        Map<String, Object> config = createConfig(DEFAULT_TIME, ".*?(\\d{8}).*", "yyyyMMdd");
        strategy.configure(config);

        // Filename has yyyy-MM-dd format but config expects yyyyMMdd
        LocalDate tenDaysAgo = LocalDate.now().minusDays(10);
        String filename = String.format("logs-%s.log", tenDaysAgo.format(DateTimeFormatter.ofPattern("yyyy-MM-dd")));
        FileObjectContext context = createContext(filename);

        assertFalse("Should not complete when date format doesn't match",
                    strategy.shouldComplete(context));
    }

    @Test
    public void testShouldNotCompleteWhenPatternDoesNotMatch() {
        Map<String, Object> config = createConfig(DEFAULT_TIME, "prefix-(\\d{4}-\\d{2}-\\d{2})-suffix", null);
        strategy.configure(config);

        // Filename doesn't match the strict pattern
        LocalDate tenDaysAgo = LocalDate.now().minusDays(10);
        String filename = String.format("logs-%s.log", tenDaysAgo.format(DateTimeFormatter.ofPattern("yyyy-MM-dd")));
        FileObjectContext context = createContext(filename);

        assertFalse("Should not complete when pattern doesn't match",
                    strategy.shouldComplete(context));
    }

    @Test
    public void testShouldCompleteForOldFile() {
        Map<String, Object> config = createConfig("01:00:00", null, null);
        strategy.configure(config);

        // File from 10 days ago should definitely be complete
        // (completion was 9 days ago at 01:00:00)
        LocalDate tenDaysAgo = LocalDate.now().minusDays(10);
        String filename = String.format("logs-%s.log", tenDaysAgo.format(DateTimeFormatter.ofPattern("yyyy-MM-dd")));
        FileObjectContext context = createContext(filename);

        assertTrue("Old file should be marked as complete",
                   strategy.shouldComplete(context));
    }

    @Test
    public void testShouldNotCompleteForFutureFile() {
        Map<String, Object> config = createConfig("01:00:00", null, null);
        strategy.configure(config);

        // File from 5 days in the future
        // (completion is 4 days in the future at 01:00:00)
        LocalDate fiveDaysFromNow = LocalDate.now().plusDays(5);
        String filename = String.format("logs-%s.log", fiveDaysFromNow.format(DateTimeFormatter.ofPattern("yyyy-MM-dd")));
        FileObjectContext context = createContext(filename);

        assertFalse("Future file should not be marked as complete",
                    strategy.shouldComplete(context));
    }


    @Test
    public void testFileCompletesNextDayAtScheduledTime() {
        Map<String, Object> config = createConfig("02:00:00", null, null);
        strategy.configure(config);

        // File from today with completion time 02:00:00
        // Should complete tomorrow at 02:00:00
        // Since we're testing today, it should NOT be complete yet
        LocalDate today = LocalDate.now();
        String filename = String.format("file-%s.log", today.format(DateTimeFormatter.ofPattern("yyyy-MM-dd")));
        FileObjectContext context = createContext(filename);

        assertFalse("Today's file should NOT be complete (completes tomorrow at 02:00:00)",
                    strategy.shouldComplete(context));
    }

    @Test
    public void testShouldAttemptReadWhenFileIsComplete() {
        Map<String, Object> config = createConfig("01:00:00", null, null);
        strategy.configure(config);

        // Old file from 10 days ago that should be complete
        long fixedTime = 1705334400000L; // 2024-01-15 12:00:00 UTC
        LocalDate tenDaysAgo = LocalDate.now().minusDays(10);
        String filename = String.format("logs-%s.log", tenDaysAgo.format(DateTimeFormatter.ofPattern("yyyy-MM-dd")));

        GenericFileObjectMeta meta = new GenericFileObjectMeta.Builder()
                .withUri(URI.create("file:///tmp/" + filename))
                .withName(filename)
                .withContentLength(1000L)
                .withLastModified(fixedTime)
                .build();

        FileObjectOffset offset = new FileObjectOffset(0, 0, fixedTime);

        assertTrue("Should attempt to read when file is complete",
                   strategy.shouldAttemptRead(meta, offset));
    }

    @Test
    public void testShouldAttemptReadWhenFileIsModified() {
        Map<String, Object> config = createConfig("23:59:59", null, null);
        strategy.configure(config);

        // File from 5 days in future - not complete yet but modified
        long fileModifiedTime = 1705334400000L; // 2024-01-15 12:00:00 UTC
        long offsetTime = 1705330800000L; // 2024-01-15 11:00:00 UTC (1 hour earlier)

        LocalDate fiveDaysFromNow = LocalDate.now().plusDays(5);
        String filename = String.format("logs-%s.log", fiveDaysFromNow.format(DateTimeFormatter.ofPattern("yyyy-MM-dd")));

        GenericFileObjectMeta meta = new GenericFileObjectMeta.Builder()
                .withUri(URI.create("file:///tmp/" + filename))
                .withName(filename)
                .withContentLength(1000L)
                .withLastModified(fileModifiedTime)
                .build();

        // Offset from 1 hour ago - file was modified after offset
        FileObjectOffset offset = new FileObjectOffset(0, 0, offsetTime);

        assertTrue("Should attempt to read when file is modified",
                   strategy.shouldAttemptRead(meta, offset));
    }

    @Test
    public void testShouldNotAttemptReadWhenFileIsNotModifiedAndNotComplete() {
        Map<String, Object> config = createConfig("23:59:59", null, null);
        strategy.configure(config);

        // File from 5 days in future - not complete yet and not modified
        LocalDate fiveDaysFromNow = LocalDate.now().plusDays(5);
        String filename = String.format("logs-%s.log", fiveDaysFromNow.format(DateTimeFormatter.ofPattern("yyyy-MM-dd")));

        long fileModifiedTime = 1705330800000L; // 2024-01-15 11:00:00 UTC
        long offsetTime = 1705334400000L; // 2024-01-15 12:00:00 UTC (newer than file)

        GenericFileObjectMeta meta = new GenericFileObjectMeta.Builder()
                .withUri(URI.create("file:///tmp/" + filename))
                .withName(filename)
                .withContentLength(1000L)
                .withLastModified(fileModifiedTime)
                .build();

        // Offset is newer than last modification
        FileObjectOffset offset = new FileObjectOffset(0, 0, offsetTime);

        assertFalse("Should not attempt to read when file is not modified and not complete",
                    strategy.shouldAttemptRead(meta, offset));
    }
}