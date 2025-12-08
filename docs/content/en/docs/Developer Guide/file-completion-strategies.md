---
date: 2025-12-18
title: "File Completion Strategies"
linkTitle: "File Completion Strategies"
weight: 95
description: >
  Learn how to control when files are marked as COMPLETED using pluggable completion strategies for long-lived files.
---

Connect File Pulse provides pluggable **File Completion Strategies** that control when files should be marked as COMPLETED. This feature is particularly useful for handling continuously appended files, such as daily log files or rolling data files.

## Overview

Previously, files were **always** marked as COMPLETED immediately when they were fully read (EOF reached). While this works well for static files, it creates challenges for files that are continuously appended, such as:

- **Daily log files** that are active throughout the day
- **Rolling files** that continue to grow until a specific time
- **Streaming data files** where you want to process data incrementally but only mark the file complete at specific times

The File Completion Strategy feature provides a pluggable architecture that allows you to customize when files should be marked as COMPLETED.

## Configuration

The completion strategy can be configured with the following connect property:

| Configuration                      | Description                                                                                    | Type    | Default                                                                         | Importance |
|------------------------------------|------------------------------------------------------------------------------------------------|---------|---------------------------------------------------------------------------------|------------|
| `fs.completion.strategy.class`     | The fully qualified name of the class which determines when files should be marked as COMPLETED | class   | `io.streamthoughts.kafka.connect.filepulse.source.EofCompletionStrategy`      | high       |

## Available Strategies

### 1. EofCompletionStrategy (Default)

**Behavior**: Marks files as COMPLETED immediately when they are fully read (End-of-File reached).

**Use Case**: Static files that don't change once created or where you want the traditional immediate completion behavior.

**Configuration**: This is the default behavior. You can explicitly configure it or simply omit the configuration.

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

### 2. DailyCompletionStrategy

**Purpose**: Mark files as COMPLETED at a scheduled time on the day **after** the file date by extracting the date from the filename.

**Use Case**: Daily log files that are continuously appended throughout the day and should only be marked as complete the next day at a specific time (e.g., 1 AM the next day).

**Important**: This strategy is designed specifically for daily files where the date in the filename represents that specific day. The file will be completed the **next day** at the scheduled time to ensure all data for that day is collected.

#### Configuration

| Configuration                          | Description                                                                      | Type     | Default                   | Importance |
|----------------------------------------|----------------------------------------------------------------------------------|----------|---------------------------|------------|
| `daily.completion.schedule.time`       | The time of day to complete files (format: HH:mm:ss)                            | string   | `00:01:00`                | high       |
| `daily.completion.schedule.date.pattern` | Regex pattern to extract date from filename (must contain one capturing group)   | string   | `.*?(\d{4}-\d{2}-\d{2}).*` | high       |
| `daily.completion.schedule.date.format` | Date format pattern for parsing the extracted date                              | string   | `yyyy-MM-dd`              | high       |

**Note**: The strategy uses the system default timezone

#### Example Configuration

```json
{
  "name": "daily-logs-connector",
  "connector.class": "io.streamthoughts.kafka.connect.filepulse.source.FilePulseSourceConnector",
  "fs.listing.class": "io.streamthoughts.kafka.connect.filepulse.fs.LocalFSDirectoryListing",
  "fs.listing.directory.path": "/logs",
  "fs.listing.filters": "io.streamthoughts.kafka.connect.filepulse.fs.filter.RegexFileListFilter",
  "file.filter.regex.pattern": "app-\\d{4}-\\d{2}-\\d{2}\\.log",
  
  "fs.completion.strategy.class": "io.streamthoughts.kafka.connect.filepulse.source.DailyCompletionStrategy",
  "daily.completion.schedule.time": "01:00:00",
  "daily.completion.schedule.date.pattern": ".*?(\\d{4}-\\d{2}-\\d{2}).*",
  "daily.completion.schedule.date.format": "yyyy-MM-dd"
}
```
