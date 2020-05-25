---
date: 2020-05-25
title: "Scanning Files"
linkTitle: "Scanning Files"
weight: 30
description: >
  The commons configuration for Connect File Pulse.
---

The connector must be configured with a specific [FSDirectoryWalker](https://github.com/streamthoughts/kafka-connect-file-pulse/blob/master/connect-file-pulse-plugin/src/main/java/io/streamthoughts/kafka/connect/filepulse/scanner/local/FSDirectoryWalker.java)  
that will be responsible for scanning an input directory to find files eligible to be streamed in Kafka.

The default `FSDirectoryWalker` implementation is :

`io.streamthoughts.kafka.connect.filepulse.scanner.local.LocalFSDirectoryWalker`.

The `FilePulseSourceConnector` periodically triggers a file system scan of the directory specified in the `input.directory.path` 
connector property. Scan is executed in a background-thread invoking the configured `FSDirectoryWalker`.

## Configuring Directory Scan (using `LocalFSDirectoryWalker`)

| Configuration |   Description |   Type    |   Default |   Importance  |
| --------------| --------------|-----------| --------- | ------------- |
|`fs.scanner.class` | The class used to scan file system | class | *io.streamthoughts.kafka.connect.filepulse.scanner.local.LocalFSDirectoryWalker* | medium |
|`fs.scan.directory.path` | The input directory to scan | string | *-* | high |
|`fs.scan.interval.ms` | Time interval in milliseconds at wish the input directory is scanned | long | *10000* | high |
|`fs.scan.filters` | The comma-separated list of fully qualified class names of the filter-filters to be uses to list eligible input files| list | *-* | medium |
|`fs.recursive.scan.enable` | Boolean indicating whether local directory should be recursively scanned | boolean | *true* | medium |

## Filtering input files

You can configure one or more `FileFilter` that will be used to determine if a file should be scheduled for processing or ignored. 
All files that are filtered out are simply ignored and remain untouched on the file system until the next scan. 
At the next scan, previously filtered files will be evaluated again to determine if they are now eligible for processing.

FilePulse packs with the following built-in filters :

### IgnoreHiddenFileFilter

The `IgnoreHiddenFileFilter` can be used to filter hidden files from being read.

**Configuration example**

```properties
fs.scan.filters=io.streamthoughts.kafka.connect.filepulse.scanner.local.filter.IgnoreHiddenFileListFilter
```

### LastModifiedFileFilter

The `LastModifiedFileFilter` can be used to filter files that have been modified to recently based on their last modified date property.

```properties
fs.scan.filters=io.streamthoughts.kafka.connect.filepulse.scanner.local.filter.LastModifiedFileFilter
# The last modified time for a file can be accepted (default: 5000)
file.filter.minimum.age.ms=10000
```

### RegexFileFilter

The `RegexFileFilter` can be used to filter files that do not match the specified regex.

```properties
fs.scan.filters=io.streamthoughts.kafka.connect.filepulse.scanner.local.filter.RegexFileFilter
# The regex pattern used to matches input files
file.filter.regex.pattern="\\.log$"
```

## Supported File types

`LocalFSDirectoryWalker` will try to detect if a file needs to be decompressed by probing its content type or its extension (javadoc : [Files#probeContentType](https://docs.oracle.com/javase/8/docs/api/java/nio/file/Files.html#probeContentType-java.nio.file.Path))

The connector supports the following content types :

* **GIZ** : `application/x-gzip`
* **TAR** : `application/x-tar`
* **ZIP** : `application/x-zip-compressed` or `application/zip`