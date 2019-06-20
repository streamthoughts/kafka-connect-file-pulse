# Configuration

## Commons configuration

Whatever the kind of files you are processing a connector should always be configured with the below properties.
Those configuration are described in detail in subsequent chapters.

| Configuration |   Description |   Type    |   Default |   Importance  |
| --------------| --------------|-----------| --------- | ------------- |
|`fs.scanner.class` | The class used to scan file system | class | *io.streamthoughts.kafka.connect.filepulse.scanner.local.LocalFSDirectoryWalker* | medium |
|`fs.cleanup.policy.class` | The class used by tasks to read input files | class | *-* | high |
|`fs.scanner.filters` | Filters use to list eligible input files| list | *-* | medium |
|`filters` | List of filters aliases to apply on each data (order is important) | list | *-* | medium |
|`input.directory.path` | The input directory to scan | string | *-* | high |
|`input.directory.scan.interval.ms` | Time interval in milliseconds at wish the input directory is scanned | long | *10000* | high |
|`internal.kafka.reporter.topic` | Name of the internal topic used by tasks and connector to report and monitor file progression. | class | *connect-file-pulse-status* | high |
|`internal.kafka.reporter.id` | Group id the internal topic used by tasks and connector to report and monitor file progression | string | *-* | high |
|`internal.kafka.reporter.cluster.bootstrap.servers` | Reporter identifier to be used by tasks and connector to report and monitor file progression (must be unique per connector). | string | *-* | high |
|`task.reader.class` | The class used by tasks to read input files | class | *io.streamthoughts.kafka.connect.filepulse.reader.RowFileReader* | high |
|`offset.strategy` | The strategy to use for building an startPosition from an input file; must be one of [name, path, name+hash] | string | *name+hash* | high |
|`topic` | The output topic to write | string | *-* | high |

{% include_relative plan.md %}