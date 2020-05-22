---
date: 2020-05-21
title: "Scanning Files"
linkTitle: "Scanning Files"
weight: 30
description: >
  The commons configuration for Connect File Pulse.
---

The connector can be configured with a specific [FSDirectoryWalker](https://github.com/streamthoughts/kafka-connect-file-pulse/blob/master/connect-file-pulse-plugin/src/main/java/io/streamthoughts/kafka/connect/filepulse/scanner/local/FSDirectoryWalker.java) 
implementation that will be responsible to scan an input directory looking for files to stream into Kafka.

The default `FSDirectoryWalker` implementation is :

`io.streamthoughts.kafka.connect.filepulse.scanner.local.LocalFSDirectoryWalker`.

When scheduled, the  `LocalFSDirectoryWalker` will recursively scan the input directory configured via  `input.directory.path`.
The SourceConnector will run a background-thread to periodically trigger a file system scan using the configured FSDirectoryWalker.

## Connector Configuration

| Configuration |   Description |   Type    |   Default |   Importance  |
| --------------| --------------|-----------| --------- | ------------- |
|`fs.scanner.class` | The class used to scan file system | class | *io.streamthoughts.kafka.connect.filepulse.scanner.local.LocalFSDirectoryWalker* | medium |
|`fs.scan.directory.path` | The input directory to scan | string | *-* | high |
|`fs.scan.interval.ms` | Time interval in milliseconds at wish the input directory is scanned | long | *10000* | high |

## Filter files

Files can be filtered to determine if they need to be scheduled or ignored. Files which are filtered are simply skipped and 
keep untouched on the file system until next scan. On the next scan, previously filtered files will be evaluate again to determine if there are now eligible to be processing.

These filters are available for use with Kafka Connect File Pulse:

| Filter | Description |
|---     | --- |
| IgnoreHiddenFileFilter | Filters hidden files from being read. |
| LastModifiedFileFilter | Filters files that been modified to recently based on their last modified date property |
| RegexFileFilter | Filter file that do not match the specified regex |


## Supported File types

`LocalFSDirectoryWalker` will try to detect if a file needs to be decompressed by probing its content type or its extension (javadoc : [Files#probeContentType](https://docs.oracle.com/javase/8/docs/api/java/nio/file/Files.html#probeContentType-java.nio.file.Path)

The connector supports the following content types :

* **GIZ** : `application/x-gzip`
* **TAR** : `application/x-tar`
* **ZIP** : `application/x-zip-compressed` or `application/zip`