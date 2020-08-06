---
date: 2020-05-25
title: "Basic Configuration"
linkTitle: "Basic Configuration"
weight: 20
description: >
  The commons configuration for deploying a File Pulse connector.
---

## Commons configuration

Whatever the kind of files you are processing a connector should always be configured with the below properties.
Those configuration are described in detail in subsequent chapters.

| Configuration |   Description |   Type    |   Default |   Importance  |
| --------------| --------------|-----------| --------- | ------------- |
|`fs.scanner.class` | The fully qualified name of the class which is used to scan file system | class | *io.streamthoughts.kafka.connect.filepulse.scanner.local.LocalFSDirectoryWalker* | medium |
|`fs.cleanup.policy.class` | The fully qualified name of the class which is used to cleanup files | class | *-* | high |
|`fs.scan.directory.path` | The input directory to scan | string | *-* | high |
|`fs.scan.interval.ms` | Time interval (in milliseconds) at wish to scan input directory | long | *10000* | high |
|`fs.scan.filters` | Filters use to list eligible input files| list | *-* | medium |
|`filters` | List of filters aliases to apply on each data (order is important) | list | *-* | medium |
|`internal.kafka.reporter.topic` | Name of the internal topic used by tasks and connector to report and monitor file progression. | class | *connect-file-pulse-status* | high |
|`internal.kafka.reporter.bootstrap.servers` |A list of host/port pairs uses by the reporter for establishing the initial connection to the Kafka cluster. | string | *-* | high |
|`task.reader.class` | The fully qualified name of the class which is used by tasks to read input files | class | *io.streamthoughts.kafka.connect.filepulse.reader.RowFileReader* | high |
|`offset.strategy` | The strategy to use for building source offset from an input file; must be one of [name, path, name+hash] | string | *name+hash* | high |
|`topic` | The default output topic to write | string | *-* | high |


### Prior to Connect FilePulse 1.3.x (deprecated)
| Configuration |   Description |   Type    |   Default |   Importance  |
| --------------| --------------|-----------| --------- | ------------- |
|`internal.kafka.reporter.id` | The reporter identifier to be used by tasks and connector to report and monitor file progression (default null). This property must only be set for users that have run a connector in version prior to 1.3.x to ensure backward-compatibility (when set, must be unique for each connect instance). | string | *-* | high |

