---
date: 2026-03-23
title: "File Completion Strategies"
linkTitle: "File Completion Strategies"
weight: 95
description: >
  Learn how to control when files are marked as COMPLETED using pluggable completion strategies for long-lived files.
---

Connect File Pulse provides pluggable **File Completion Strategies** that control when files should be marked as COMPLETED. This feature is particularly useful for handling continuously appended files, such as daily log files, weekly reports, or monthly data exports.

## Overview

By default, files are marked as COMPLETED immediately when they are fully read (EOF reached). While this works well for static files, it creates challenges for files that are continuously appended, such as:

- **Daily log files** that are active throughout the day
- **Weekly report files** that accumulate data over a full week
- **Monthly data exports** that are written incrementally over a month
- **Rolling files** that continue to grow until a specific time

The File Completion Strategy feature provides a pluggable architecture that allows you to customize when files should be marked as COMPLETED.

## Configuration

The completion strategy is configured with the following connector property:

| Configuration                  | Description                                                                                     | Type  | Default                                                                     | Importance |
|--------------------------------|-------------------------------------------------------------------------------------------------|-------|-----------------------------------------------------------------------------|------------|
| `fs.completion.strategy.class` | Fully qualified class name of the strategy that determines when files are marked as COMPLETED. | class | `io.streamthoughts.kafka.connect.filepulse.source.EofCompletionStrategy`   | high       |

## Available Strategies

### 1. EofCompletionStrategy (Default)

**Behavior**: Marks files as COMPLETED immediately when they are fully read (End-of-File reached).

**Use Case**: Static files that don't change once created, or when you want the traditional immediate-completion behavior.

```json
{
  "fs.completion.strategy.class": "io.streamthoughts.kafka.connect.filepulse.source.EofCompletionStrategy"
}
```

**Characteristics**:
- ✅ Simple and predictable behavior
- ✅ Immediate file completion upon reading all content
- ✅ Suitable for batch processing of static files

---

### 2. ScheduledCompletionStrategy

**Behavior**: Marks files as COMPLETED at a scheduled time relative to the date extracted from the filename.
The strategy supports three period granularities controlled by the `completion.schedule.period` option:

| Period    | Completes at                                              | Example (file date `2026-03-10`, time `01:00:00`)  |
|-----------|-----------------------------------------------------------|----------------------------------------------------|
| `DAILY`   | Day after the file date @ scheduled time                  | `2026-03-11 01:00:00`                              |
| `WEEKLY`  | First day of the next week (configurable start day) after the file date @ scheduled time | `2026-03-16 01:00:00` (Monday start)  |
| `MONTHLY` | First day of the next month after the file date @ scheduled time | `2026-04-01 01:00:00`                       |

**Use Case**: Long-lived files that are continuously appended over a day, a week, or a month and should only be marked as complete once that period has ended.

This strategy also implements a **read back-off** mechanism: when the scheduled completion time has not yet been reached and the file has not been modified since the last read, the connector defers the next read attempt. This avoids unnecessary polling of files that are not expected to have new data yet.

> **Note**: All times are resolved using the **system default timezone** of the JVM running the connector.

#### Configuration

| Configuration                         | Description                                                                                          | Type   | Default                       | Importance |
|---------------------------------------|------------------------------------------------------------------------------------------------------|--------|-------------------------------|------------|
| `completion.schedule.period`          | Period granularity: `DAILY`, `WEEKLY`, or `MONTHLY`.                                                | string | `DAILY`                       | high       |
| `completion.schedule.time`            | Time of day at which to mark the file as COMPLETED (format: `HH:mm:ss`).                           | string | `00:01:00`                    | high       |
| `completion.schedule.date.pattern`    | Regex pattern to extract the date from the filename. Must contain exactly one capturing group.       | string | `.*?(\d{4}-\d{2}-\d{2}).*`   | high       |
| `completion.schedule.date.format`     | `DateTimeFormatter` pattern used to parse the date captured by the regex group.                     | string | `yyyy-MM-dd`                  | high       |
| `completion.schedule.week.start.day`  | First day of the week used by the `WEEKLY` period when no day field is present in the date format. Accepted values: `MONDAY`, `TUESDAY`, `WEDNESDAY`, `THURSDAY`, `FRIDAY`, `SATURDAY`, `SUNDAY`. | string | `MONDAY` | low |

#### Examples

**Daily** — complete daily log files (e.g. log-2026-03-25.log) the next day at 01:00:

```json
{
  "fs.completion.strategy.class": "io.streamthoughts.kafka.connect.filepulse.source.ScheduledCompletionStrategy",
  "completion.schedule.period": "DAILY",
  "completion.schedule.time": "01:00:00",
  "completion.schedule.date.pattern": ".*?(\\d{4}-\\d{2}-\\d{2}).*",
  "completion.schedule.date.format": "yyyy-MM-dd"
}
```

**Weekly** — complete weekly report files (e.g. `report-2026-W11.log`) the following Monday at 06:00:

```json
{
  "fs.completion.strategy.class": "io.streamthoughts.kafka.connect.filepulse.source.ScheduledCompletionStrategy",
  "completion.schedule.period": "WEEKLY",
  "completion.schedule.time": "06:00:00",
  "completion.schedule.date.pattern": ".*?(\\d{4}-W\\d{2}).*",
  "completion.schedule.date.format": "YYYY-'W'ww"
}
```

**Weekly (Sunday start)** — same as above but completing on the following Sunday at 06:00:

```json
{
  "fs.completion.strategy.class": "io.streamthoughts.kafka.connect.filepulse.source.ScheduledCompletionStrategy",
  "completion.schedule.period": "WEEKLY",
  "completion.schedule.time": "06:00:00",
  "completion.schedule.date.pattern": ".*?(\\d{4}-W\\d{2}).*",
  "completion.schedule.date.format": "YYYY-'W'ww",
  "completion.schedule.week.start.day": "SUNDAY"
}
```

**Monthly** — complete monthly exports (e.g. `export-2026-03.log`) on the 1st of the next month at 02:00:

```json
{
  "fs.completion.strategy.class": "io.streamthoughts.kafka.connect.filepulse.source.ScheduledCompletionStrategy",
  "completion.schedule.period": "MONTHLY",
  "completion.schedule.time": "02:00:00",
  "completion.schedule.date.pattern": ".*?(\\d{4}-\\d{2}).*",
  "completion.schedule.date.format": "yyyy-MM"
}
```

> **Note on partial date formats**: formats that do not include a full date (e.g. `yyyy-MM` or `YYYY-'W'ww`) are
> supported. The missing day field defaults to `1` (first day of the month for `MONTHLY`, or the configured
> `completion.schedule.week.start.day` — Monday by default — for `WEEKLY`), which is sufficient for the
> period boundary calculation.

---

### 3. DailyCompletionStrategy *(Deprecated)*

> ⚠️ **Deprecated** — use [`ScheduledCompletionStrategy`](#2-scheduledcompletionstrategy) with `completion.schedule.period=DAILY` instead.

`DailyCompletionStrategy` is kept for backward compatibility. It behaves identically to `ScheduledCompletionStrategy` with `DAILY` period and transparently remaps the legacy `daily.completion.schedule.*` config keys to the new `completion.schedule.*` keys — so **existing connector configurations require no change**.

#### Legacy configuration keys (still accepted)

| Legacy key                              | Maps to                              |
|-----------------------------------------|--------------------------------------|
| `daily.completion.schedule.time`        | `completion.schedule.time`           |
| `daily.completion.schedule.date.pattern`| `completion.schedule.date.pattern`   |
| `daily.completion.schedule.date.format` | `completion.schedule.date.format`    |

#### Migration

Replace the class and rename the config keys:

```json
{
  "fs.completion.strategy.class": "io.streamthoughts.kafka.connect.filepulse.source.ScheduledCompletionStrategy",
  "completion.schedule.period": "DAILY",
  "completion.schedule.time": "01:00:00",
  "completion.schedule.date.pattern": ".*?(\\d{4}-\\d{2}-\\d{2}).*",
  "completion.schedule.date.format": "yyyy-MM-dd"
}
```

---

## Implementing a Custom Strategy

You can implement your own strategy by implementing the `FileCompletionStrategy` interface:

```java
public interface FileCompletionStrategy {

    default void configure(Map<String, ?> configs) { }

    boolean shouldComplete(FileObjectContext context);
}
```

Optionally, also implement `LongLivedFileReadStrategy` to control whether the connector should attempt to read from the file at a given moment (useful to avoid unnecessary polling):

```java
public interface LongLivedFileReadStrategy {

    default boolean shouldAttemptRead(FileObjectMeta objectMeta, FileObjectOffset offset) {
        return objectMeta.lastModified() > offset.timestamp();
    }
}
```

Set your implementation class on the `fs.completion.strategy.class` connector property.
