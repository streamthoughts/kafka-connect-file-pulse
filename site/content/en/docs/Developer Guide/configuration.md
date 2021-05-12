---
date: 2021-05-12
title: "Configuration"
linkTitle: "Configuration"
weight: 20
description: >
  The common configurations for deploying a File Pulse connector.
---

## Commons configuration

Whatever the kind of files you are processing a connector should always be configured with the below properties.
These configurations are described in detail in subsequent chapters.

**Common Kafka Connect properties**

| Configuration |   Description |   Type    |   Default |   Importance  |
| --------------| --------------|-----------| --------- | ------------- |
| `topic` | The default output topic to write | string | *-* | high |
| `tasks.max` | The maximum number of tasks that should be created for this connector.  | string | *-* | high |

**Properties for listing and cleaning object files ([FileSystemListing](/kafka-connect-file-pulse/docs/developer-guide/file-listing/))**

| Configuration |   Description |   Type    |   Default |   Importance  |
| --------------| --------------|-----------| --------- | ------------- |
| `fs.listing.class` | Class which is used to list eligible files from the scanned file system. | class | *io.streamthoughts.kafka.connect.filepulse.scanner.local.LocalFSDirectoryWalker* | MEDIUM |
| `fs.listing.filters` | Filters use to list eligible input files| list | *-* | MEDIUM |
| `fs.listing.interval.ms` | Time interval (in milliseconds) at wish to scan input directory | long | *10000* | HIGH |
| `fs.cleanup.policy.class` | The fully qualified name of the class which is used to cleanup files | class | *-* | HIGH |
| `max.scheduled.files` | Maximum number of files that can be schedules to tasks. | long | *1000* | HIGH |

**Properties for transforming object file record([Filters Chain Definition](/kafka-connect-file-pulse/docs/developer-guide/filters-chain-definition/))**

| Configuration |   Description |   Type    |   Default |   Importance  |
| --------------| --------------|-----------| --------- | ------------- |
| `filters` | List of filters aliases to apply on each data (order is important) | list | *-* | MEDIUM |

**Properties for reading object file record([FileReaders](/kafka-connect-file-pulse/docs/developer-guide/file-readers/))**

| Configuration |   Description |   Type    |   Default |   Importance  |
| --------------| --------------|-----------| --------- | ------------- |
| `tasks.reader.class` | The fully qualified name of the class which is used by tasks to read input files | class | *io.streamthoughts.kafka.connect.filepulse.reader.RowFileReader* | HIGH |

**Properties for uniquely identifying object files and records ([FileReaders](/kafka-connect-file-pulse/docs/developer-guide/file-readers/))**

| `offset.policy.class` | Class which is used to determine the source partition and offset that uniquely identify a input record | `class` | *io.streamthoughts.kafka.connect.filepulse.offset.DefaultSourceOffsetPolicy* | HIGH |

**Properties for synchronizing Connector and Tasks**

| Configuration |   Description |   Type    |   Default |   Importance  |
| --------------| --------------|-----------| --------- | ------------- |
| `internal.kafka.reporter.topic` | Name of the internal topic used by tasks and connector to report and monitor file progression. | class | *connect-file-pulse-status* | HIGH |
| `internal.kafka.reporter.bootstrap.servers` |A list of host/port pairs uses by the reporter for establishing the initial connection to the Kafka cluster. | string | *-* | HIGH |

### Prior to Connect FilePulse 1.3.x (deprecated)

| Configuration |   Description |   Type    |   Default |   Importance  |
| --------------| --------------|-----------| --------- | ------------- |
|`internal.kafka.reporter.id` | The reporter identifier to be used by tasks and connector to report and monitor file progression (default null). This property must only be set for users that have run a connector in version prior to 1.3.x to ensure backward-compatibility (when set, must be unique for each connect instance). | string | *-* | HIGH |

A separated list of attributes, using `+`  as a character separator, to be used for uniquely identifying an input file; must be one of [`name`, `path`, `lastModified`, `inode`, `hash`] (e.g: `name+hash`). Note that order doesn't matter.

## Examples

Some configuration examples are available [here](https://github.com/streamthoughts/kafka-connect-file-pulse/tree/master/examples).