# Kafka Connect File Pulse

Connect File Pulse is a multi-purpose [Kafka Connect](http://kafka.apache.org/documentation.html#connect) (source) for streaming files from a local filesystem to Kafka.

This project is still in beta version. Any feedback, bug reports and PRs are greatly appreciated!

## Motivation

A lot of enterprises still rely on files to export/import data between their systems either in near real time or daily.
Files can be in different textual formats such as CSV, XML, JSON and so one.
A common use case is to decouple those systems by streaming those files into Kafka.

Connect File Pulse  attends to be a simple solution to deal with those kind of files.

Connect File Pulse is largely inspired from functionality provided by both Elasticsearch and Logstash.

## Build & Package

**Clone Git repository**
```bash
git clone https://github.com/streamthoughts/kafka-connect-file-pulse.git
cd kafka-connect-file-pulse
```

**Build & Package Connector**

You can build kafka-connect-file-pulse with Maven using standard lifecycle.

```bash
mvn clean install -DskipTests
```

Note : Connector will be package into an archive file compatible with [confluent-hub client](https://docs.confluent.io/current/connect/managing/confluent-hub/client.html) :
```
./connect-file-pulse-plugin/target/components/packages/streamthoughts-kafka-connect-file-pulse-plugin-<FILEPULSE_VERSION>.zip
```

## Quick-start

**1 ) Run Confluent Platforms with Connect File Pulse**

```bash
docker-compose build
docker-compose up -d
```

**2 ) Check for Kafka Connect**
```
docker logs --tail="all" -f connect"
```

**3 ) Verify that Connect File Pulse plugins correctly loaded**
```bash
curl -sX GET http://localhost:8083/connector-plugins | grep FilePulseSourceConnector
```


### Example : Logs Parsing (Log4j)

This example starts a new connector instance to parse the Kafka Connect container log4j logs before writing them into a configured topic.


**1 ) Start a new connector instance**

```bash
curl -sX POST http://localhost:8083/connectors \
-d @config/connect-file-pulse-quickstart-log4j.json \
--header "Content-Type: application/json" | jq
```

**2 ) Check connector status**
```bash
curl -X GET http://localhost:8083/connectors/connect-file-pulse-quickstart-log4j | jq
```

**3 ) Consume output topics**
```bash
docker exec -it -e KAFKA_OPTS="" connect kafka-avro-console-consumer --topic connect-file-pulse-quickstart-log4j --from-beginning --bootstrap-server broker:29092 --property schema.registry.url=http://schema-registry:8081
```

(output)
```json
...
{"loglevel":{"string":"INFO"},"logdate":{"string":"2019-06-16 20:41:15,247"},"message":{"string":"[main] Scanning for plugin classes. This might take a moment ... (org.apache.kafka.connect.cli.ConnectDistributed)"}}
{"loglevel":{"string":"INFO"},"logdate":{"string":"2019-06-16 20:41:15,270"},"message":{"string":"[main] Loading plugin from: /usr/share/java/schema-registry (org.apache.kafka.connect.runtime.isolation.DelegatingClassLoader)"}}
{"loglevel":{"string":"INFO"},"logdate":{"string":"2019-06-16 20:41:16,115"},"message":{"string":"[main] Registered loader: PluginClassLoader{pluginLocation=file:/usr/share/java/schema-registry/} (org.apache.kafka.connect.runtime.isolation.DelegatingClassLoader)"}}
{"loglevel":{"string":"INFO"},"logdate":{"string":"2019-06-16 20:41:16,115"},"message":{"string":"[main] Added plugin 'org.apache.kafka.common.config.provider.FileConfigProvider' (org.apache.kafka.connect.runtime.isolation.DelegatingClassLoader)"}}
...
```

**4) Observe Connect status**

Connect File Pulse use an internal topic to track the current state of files being processing.

```bash
docker exec -it -e KAFKA_OPTS="" connect kafka-console-consumer --topic connect-file-pulse-status --from-beginning --bootstrap-server broker:29092
```

(output)
```json
{"hostname":"f51d45f96ed5","status":"SCHEDULED","metadata":{"name":"kafka-connect.log","path":"/var/log/kafka","size":172559,"lastModified":1560772525000,"inode":1705406,"hash":661976312},"offset":{"position":-1,"rows":0,"timestamp":1560772525527}}
{"hostname":"f51d45f96ed5","status":"STARTED","metadata":{"name":"kafka-connect.log","path":"/var/log/kafka","size":172559,"lastModified":1560772525000,"inode":1705406,"hash":661976312},"offset":{"position":-1,"rows":0,"timestamp":1560772525719}}
{"hostname":"f51d45f96ed5","status":"READING","metadata":{"name":"kafka-connect.log","path":"/var/log/kafka","size":172559,"lastModified":1560772525000,"inode":1705406,"hash":661976312},"offset":{"position":174780,"rows":1911,"timestamp":1560772535322}}
...
```

**5 ) Stop all containers**
```bash
docker-compose down
```

### Example : CSV File Parsing

This example starts a new connector instance that parse a CSV file and filter rows based on column's values before writing record into Kafka.

**1 ) Start a new connector instance**

```bash
curl -sX POST http://localhost:8083/connectors \
-d @config/connect-file-pulse-quickstart-csv.json \
--header "Content-Type: application/json" | jq
```

**2 ) Copy example csv file into container**

```bash
docker exec -it connect mkdir -p /tmp/kafka-connect/examples
docker cp examples/quickstart-musics-dataset.csv connect://tmp/kafka-connect/examples/quickstart-musics-dataset.csv
```

**3 ) Check connector status**
```bash
curl -X GET http://localhost:8083/connectors/connect-file-pulse-quickstart-csv | jq
```

**4 ) Check for task completion**
```
docker logs --tail="all" -f connect | grep "Orphan task detected"
```

**5 ) Consume output topics**
```bash
docker exec -it connect kafka-avro-console-consumer --topic connect-file-pulse-quickstart-csv --from-beginning --bootstrap-server broker:29092 --property schema.registry.url=http://schema-registry:8081
```

## Documentation

1. [Common Configuration](#common-configuration)
2. [Scanning Files](#scanning-files)
3. [File Readers](#file-readers)
4. [Filters Chain definition](filters-chain-definitions)
5. [Difference between Kafka Connect Single Message Transforms (SMT) functionality](#difference-between-kafka-connect-single-message-transforms-smt-functionality)
6. [Acceding Data and Metadata](#acceding-data-and-metadata)
7. [Conditional execution](conditional-execution)
8. [Handling Failures in Pipelines](#handling-failires-in-pipelines)
9. [Filters](#filters)
10. [Tracking file progression](#tracking-file-progression)
11. [Cleaning completed files](#cleaning-completed-files)

## Common configuration

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


## Scanning Files

The connector can be configured with a specific [FSDirectoryWalker](src/main/java/io/streamthoughts/kafka/connect/filepulse/scanner/FSDirectoryWalker) implementation that will be responsible to scan an input directory looking for files to stream into Kafka.

The default `FSDirectoryWalker` implementation is :

`io.streamthoughts.kafka.connect.filepulse.scanner.local.LocalFSDirectoryWalker`.

When scheduled, the  `LocalFSDirectoryWalker` will recursively scan the input directory configured via  `input.directory.path`.
The SourceConnector will run a background-thread to periodically trigger a file system scan using the configured FSDirectoryWalker.

### Connector Configuration

| Configuration |   Description |   Type    |   Default |   Importance  |
| --------------| --------------|-----------| --------- | ------------- |
|`fs.scanner.class` | The class used to scan file system | class | *io.streamthoughts.kafka.connect.filepulse.scanner.local.LocalFSDirectoryWalker* | medium |
|`input.directory.path` | The input directory to scan | string | *-* | high |
|`input.directory.scan.interval.ms` | Time interval in milliseconds at wish the input directory is scanned | long | *10000* | high |

### Filter files

Files can be filtered to determine if they need to be scheduled or ignored. Files which are filtered are simply skipped and  keep untouch on the file system until next scan. On the next scan, previously filtered files will be evaluate again to determine if there are now eligible to be processing.

These filters are available for use with Kafka Connect File Pulse:

| Filter | Description |
|---     | --- |
| IgnoreHiddenFileFilter | Filters hidden files from being read.
| LastModifiedFileFilter | Filters files that been modified to recently based on their last modified date property
| RegexFileFilter | Filter file that do not match the specified regex


### Supported File types

`LocalFSDirectoryWalker` will try to detect if a file needs to be decompressed by probing its content type or its extension (javadoc : [Files#probeContentType](https://docs.oracle.com/javase/8/docs/api/java/nio/file/Files.html#probeContentType-java.nio.file.Path)


The connector supports the following content types :

* **GIZ** : `application/x-gzip`
* **TAR** : `application/x-tar`
* **ZIP** : `application/x-zip-compressed` or `application/zip`

## File Readers

The connector can be configured with a specific FileInputReader.
Currently, it only supports the `RowFileInputReader` that will read a file from the local file system line by line.

### RowFileInputReader

The following provides usage information for [FileInputReader](blob/master/src/main/java/io/streamthoughts/kafka/connect/filepulse/reader/FileInputReader.java)
:  `io.streamthoughts.kafka.connect.filepulse.reader.impl.RowFileInputReader` ([source code](blob/master/src/main/java/io/streamthoughts/kafka/connect/filepulse/reader/RowFileInputReader.java))



#### Configuration

| Configuration |   Description |   Type    |   Default |   Importance  |
| --------------| --------------|-----------| --------- | ------------- |
|`file.encoding` | The text file encoding to use  | string |*UTF_8* |  high
|`buffer.initial.bytes.size` | The initial buffer size used to read input files | string |*4096* |  high
|`min.read.records` | The minimum number of records to read from file before returning to task. | integer |*1* |  high
|`skip.headers` | The number of rows to be skipped in the beginning of file | string |*0* |  high
|`skip.footers` | The number of rows to be skipped at the end of file | string |*0* |  high

## Filters Chain definition

The connector can be configured to apply complex transformations on messages before they are written to Kafka.

A [filter](#filters) chain can be specified in the connector configuration.

 * filters - List of aliases for the filter, specifying the order in which the filters will be applied.
 * filters.$alias.type - Fully qualified class name for the filter.
 * filters.$alias.$filterSpecificConfig Configuration properties for the filter

For example, let's parse a standard application logs file written with log4j using the build-in filters :

```
filters=GroupMultilineException, ExtractFirstLine, ParseLog4jLog

filters.GroupMultilineException.type=io.streamthoughts.kafka.connect.filepulse.filter.MultiRowFilter
filters.GroupMultilineException.negate=false
filters.GroupMultilineException.pattern="^[\\t]"

filters.ExtractFirstLine.type=io.streamthoughts.kafka.connect.filepulse.filter.AppendFilter
filters.ExtractFirstLine.field=logmessage
filters.ExtractFirstLine.values=${extractarray(message,0)}

filters.ParseLog4jLog.type=io.streamthoughts.kafka.connect.filepulse.filter.impl.GrokFilter
filters.ParseLog4jLog.match="%{TIMESTAMP_ISO8601:logdate} %{LOGLEVEL:loglevel} %{GREEDYDATA:thread} %{GREEDYDATA:logmessage}"
filters.ParseLog4jLog.source=log
filters.ParseLog4jLog.overwrite=logmessage
```


These filters are available for use with Kafka Connect File Pulse:


| Filter | Description |
|---     | --- |
| [AppendFilter](#appendfilter) | Appends one or more values to an existing or non-existing array field  |
| [ConvertFilter](#convertfilter)  | Converts a message field's value to a specific type |
| [DelimitedRowFilter](#delimitedrowfilter)  | Parses a message field's value containing columns delimited by a separator into a struct |
| [GrokFilter](#grokfilter)  | Parses an unstructured message field's value to a struct by combining Grok patterns |
| [GroupRowFilter](#grouprowfilter)  | Regroups multiple following messages into a single message by composing a grouping key|
| [JSONFilter](#jsonfilter)  | Unmarshallings a JSON message field's value to a complex struct |
| [MultiRowFilter](#multirowfilter)  | Combines following message lines into single one by combining patterns |
| [RenameFilter](#renamefilter)  | Renames a message field |
| [SplitFilter](#splitfilter)  | Splits a message field's value to array |


## Difference between Kafka Connect Single Message Transforms (SMT) functionality

Filters can be compared to Kafka Connect built-in [Transformers](https://kafka.apache.org/documentation/#connect_transforms).
However, filters allow more complex pipelines to be built for structuring file data.
For example, they can be used to split one input message to multiple messages or to temporarily buffer consecutive messages in order to regroup them by fields or a pattern.

## Acceding Data and Metadata

Some filter properties (like : [AppendFilter](#appendfilter)) can be configured using *Simple Connect Expression Language*.

*Simple Connect Expression Language* (ScEL for short) is an expression language based on regex that allow quick access and manipulating record fields and metadata.

The syntax to define an expression is of the form : "`{{ <expression string> }}`".

ScEL supports the following functionality :

* Field Selector
* Nested Navigation
* String substitution
* Functions

#### Field Selector

Expression language can be used to easily select one field from the input record : "`{{ username }}`."

#### Nested Navigation

To navigate down a struct value, just use a period to indicate a nested field value : "`{{ address.city }}`."

#### String substitution

Expression language can be used to easily build a new string field that concatenate multiple ones : "`{{ <expression one> }}-{{ <expression two>}}`."

#### Functions

ScEL supports a number of predefined functions that can be used to apply a single transformation on a field.

| Function       | Description   | Example   |
| ---------------| --------------|-----------|
| `converts`     | Converts a field'value into the specified type | `{{converts(my_field, INTEGER) }}`
| `extract_array`| Returns the element at the specified position of the specified array | `{{extract_array(my_array, 0) }}`
| `length`       | Returns the number of elements into an array of the length of an string field | `{{ length(my_array) }}`
| `lowercase`    | Converts all of the characters in a string field's value to lower case | `{{ lowercase(my_field) }}`
| `nlv`          | Sets a default value if a field's value is null | `{{ length(my_array) }}`
| `uppercase`    | Converts all of the characters in a string field's value to upper case  | `{{ uppercase(my_field) }}`

See *Conditional execution* for additional functions.

**Limitations** :

* Current, this is not possible to register user-defined functions (UDFs).

### Pre-defined fields

Connect File Pulse pre-defined some special fields prefixed with `$`. Those fields are not part of final record write into Kafka.

| Predefined Fields / ScEL | Description | Type
|--- | --- |--- |
| `{{ $file.name }}` | The file name  | `string`
| `{{ $file.path }}` | The file directory path | `string`
| `{{ $file.absPath }}` | The file absolute path | `string`
| `{{ $file.hash }}` | The file CRC32 hash | `int`
| `{{ $file.lastModified }}` | The file last modified time.  | `long`
| `{{ $file.size }}` | The file size  | `long`
| `{{ $offset }}` | The position of the record into the source file  | `long`
| `{{ $row }}` | The row number of the record into the source | `long`

Note, that in case of failures more fields are added to the current filter context (see : [Handling Failures in Pipelines](#Handling Failures in Pipelines)


## Conditional execution

A conditional property `if` can be configured on each filter to determine if that filter should be applied or skipped.
When a filter is skipped, message flow to the next filter without any modification.

`if` configuration accepts a Simple Connect Expression that must return to `true` or `false`.
If the configured expression does not evaluate to a boolean value the filter chain will failed.

For example, the below filter will only be applied on message having a log message containing "BadCredentialsException"

```
filters.TagSecurityException.type=io.streamthoughts.kafka.connect.filepulse.filter.AppendFilter
filters.TagSecurityException.if={{ contains(data.logmessage, BadCredentialsException) }}
filters.TagSecurityException.field=tags
filters.TagSecurityException.values=SecurityAlert
```

These boolean functions are available for use with `if` configuration :

| Function      | Description   | Example   |
| --------------| --------------|-----------|
| `contains` | Returns `true` if an array field's value contains the specified value  | `{{ contains(field, value) }}`
| `ends_with`  | Returns `true` if an a string field's value end with the specified string suffix | `{{ ends_with(field, suffix) }}`
| `equals` | Returns `true` if an a string or number fields's value equals the specified value | `{{ equals(field, value) }}`
| `exists`  | Returns `true` if an the specified field exists | `{{ ends_with(field, value) }}`
| `is_null`  | Returns `true` if a field's value is null | `{{ is_null(field) }}`
| `matches` | Returns `true` if a field's value match the specified regex | `{{ matches(field, regex)`
| `starts_with` | Returns `true` if an a string field's value start with the specified string prefix | `{{ endWith(field, prefix) }}`


**Limitations** :
 * `if` property does not support binary operator and then a single condition can be configured.
 * condition cannot be used to easily create pipeline branching.

## Handling Failures in Pipelines

## Filters

### AppendFilter

The following provides usage information for : `io.streamthoughts.kafka.connect.filepulse.filter.AppendFilter`

#### Configuration

| Configuration |   Description |   Type    |   Default |   Importance  |
| --------------| --------------|-----------| --------- | ------------- |
| `field` | The field to append    | string | *-* | high |
| `value` | The value to be appended    | string | *-* | high |
| `overwrite` | Is existing field should be overwrite    | boolean | *false* | high |

#### Example

```
```

### ConvertFilter

The following provides usage information for : `io.streamthoughts.kafka.connect.filepulse.filter.ConvertFilter`

#### Configuration

| Configuration |   Description |   Type    |   Default |   Importance  |
| --------------| --------------|-----------| --------- | ------------- |
| `field` | The field to convert    | string | *-* | high |
| `type` | The type field must be converted to  | string | *,* | high |
| `ignoreMissing` | If true and field does not exist the filter will be apply successfully without modifying the data. If field is null the schema will be modified. | boolean | *true* | high |

#### Example

```
```

### DelimitedRowFilter

The following provides usage information for : `io.streamthoughts.kafka.connect.filepulse.filter.DelimitedRowFilter`

The delimited-row filter can be used to parse and stream delimited row files (i.e CSV) into Kafka.
Each row is parsed and published into a configured topic as a single Kafka data.

#### Configuration

| Configuration |   Description |   Type    |   Default |   Importance  |
| --------------| --------------|-----------| --------- | ------------- |
|`separator` | The character used as a delimiter/separator between each value | string |*;* |  high
|`trimColumn` | Remove the leading and trailing whitespaces from all columns. |  boolean | *false* | low
|`extractColumnName` | Define the field from which the schema should be detected (all columns will be of type 'string') | string | | high
|`autoGenerateColumnNames` | Define whether column names should autogenerated or not (column names will of the form 'column1, column2') | *true* | boolean | high
|`columns` | Define the list of column names in order they appear in each row. columns must be in the form of TYPE:NAME | string | | high

#### Example

```
```

### GrokFilter

The following provides usage information for : `io.streamthoughts.kafka.connect.filepulse.filter.GrokFilter`

The grok filter allows you to parse unstructured data like applications logs to extract structured and meaningful data fields.

**Regular Expressions**
Grok are built on top of on regular expressions, so you can use any regular expressions as well to define your own patterns.
The regular expression library is [Joni](https://github.com/jruby/joni), a Java port of Oniguruma regexp library

#### Configuration

| Configuration |   Description |   Type    |   Default |   Importance  |
| --------------| --------------|-----------| --------- | ------------- |
| `namedCapturesOnly` | If true, only store named captures from grok. | boolean | *true* | high
| `matches` | The Grok pattern to match. | string | *-* | high
| `overwrite` | The fields to overwrite.    | list | moderate
| `patternDefinitions` | Custom pattern definitions. | list | *-* | low
| `patternsDir` | List of user-defined pattern directories | string | *-* | low
| `source` | The input field on which to apply the filter  | string | *message* | medium

#### Example

```
```

### GroupRowFilter

The following provides usage information for : `io.streamthoughts.kafka.connect.filepulse.filter.GroupRowFilter`

#### Configuration

| Configuration |   Description |   Type    |   Default |   Importance  |
| --------------| --------------|-----------| --------- | ------------- |
| `fields` | List of fields used to regroup records | list | high
| `max.buffered.records` | The maximum number of records to group (default : -1).| integer | *-1* | high
| `target` | The target array field to put the grouped field | integer | *records* | high

#### Example

```
```

### JSONFilter

The following provides usage information for : `io.streamthoughts.kafka.connect.filepulse.filter.JSONFilter`


The JSON filter parses an input json field.

#### Configuration

| Configuration |   Description |   Type    |   Default |   Importance  |
| --------------| --------------|-----------| --------- | ------------- |
| `overwrite` | The fields to overwrite.    | list | *-* | medium |
| `source` | The input field on which to apply the filter  | string | *message* | medium |
| `target` | he target field to put the parsed JSON data  | string | *-* | high |

#### Example

```
filters=MyJsonFilter
filters.MyJsonFilter.type=io.streamthoughts.kafka.connect.filepulse.filter.JSONFilter
filters.MyJsonFilter.source=message
filters.MyJsonFilter.target=payload
```

### MultiRowFilter

The following provides usage information for : `io.streamthoughts.kafka.connect.filepulse.filter.MultiRowFilter`


The multirow filter joins multiple lines into a single defaultStruct using a regex pattern.
For example, this filter can be used for joining Java exception and stacktrace messages 

#### Configuration

| Configuration |   Description |   Type    |   Default |   Importance  |
| --------------| --------------|-----------| --------- | ------------- |
| `negate` | Negate the regexp pattern (if not matched)."   | boolean | *-* | medium
| `pattern` | The pattern to match multiline  | string | *-* | high
| `patternDefinitions` | Custom pattern definitions. | list | *-* | low
| `patternsDir` | List of user-defined pattern directories | string | *-* | low
| `separator` | The character to be used to concat multi lines  | string | "\\n" | high

#### Example

```
filters=StackTraceMultiRowFilter
filters.StackTraceMultiRowFilter.type=io.streamthoughts.kafka.connect.filepulse.filter.MultiRowFilter
filters.StackTraceMultiRowFilter.negate=false
filters.StackTraceMultiRowFilter.pattern=^[\t]
```

### RenameFilter

The following provides usage information for : `io.streamthoughts.kafka.connect.filepulse.filter.RenameFilter`

#### Configuration

| Configuration |   Description |   Type    |   Default |   Importance  |
| --------------| --------------|-----------| --------- | ------------- |
| `field` | The field to rename | string | *-* | high |
| `target` | The target name | string | *-* | high |
| `ignoreMissing` | If true and field does not exist the filter will be apply successfully without modifying the data. If field is null the schema will be modified. | boolean | *true* | high

#### Example

```
```

### SplitFilter

The following provides usage information for : `io.streamthoughts.kafka.connect.filepulse.filter.SplitFilter`

#### Configuration

| Configuration |   Description |   Type    |   Default |   Importance  |
| --------------| --------------|-----------| --------- | ------------- |
| `split` | Split a message field's value to array    | string | *-* | high |
| `separator` | The separator used for splitting a withMessage field's value to array  | string | *,* | high |
| `target` | he target field to put the parsed JSON data  | string | *-* | high |


## Tracking file progression

Connect File Pulse use an internal topic (*default:connect-file-pulse-status*) to track the current state of files being processed.
This topic is used internally by Tasks to communicate to the SourceConnector instance but you can easily use it to monitor files progression.

### The message format
Status event are publish into JSON with the following schema :

```
{
"hostname": {
    "type": "string",
    "description: "The machine from which the source file is read."
},
"status":{
    "type": "string",
    "description: "The current status"
},
"metadata":{
    "name": {
        "type": "string",
        "description: "The file name."
    },
    "path": {
        "type": "string",
        "description: "The file absolute path."
    },
    "size": {
        "type": "int",
        "description: "The file size."
    },
    lastModified": {
        "type": "long",
        "description: "The file last-modified property."
    },
    "inode": {
        "type": "int",
        "description: "The file inode"
    },
    "hash": {
        "type": "int",
        "description: "CRC32"
    }
},
"offset":{
    "position": {
        "type": "long",
        "description: "The current position in the source file (default : -1)."
    },
    "rows": {
        "type": "long",
        "description: "The number of rows already read from the source file (default : -1)."
    },
    "timestamp": {
        "type": "long",
        "description: "The offset timestamp"
    }
}
```

### Status

Source file can be in the following states :

* \[1\] **SCHEDULED** :  The file has been scheduled by the connector monitoring thread.
* \[2\] **INVALID** :  The file can't be scheduled because it is not readable.
* \[2\] **STARTED** : The file is starting to be read by a Task.
* \[3\] **READING** : The file is currently being read by a task. An event is wrote into Kafka while committing offsets.
* \[4\] **FAILED** : The file processing failed.
* \[4\] **COMPLETED** : The file processing is completed.
* \[5\] **CLEANED** :  The file has been successfully clean up (depending of the configured strategy).

### Configuration

| Configuration |   Description |   Type    |   Default |   Importance  |
| --------------| --------------|-----------| --------- | ------------- |
|`internal.kafka.reporter.topic` | Name of the internal topic used by tasks and connector to report and monitor file progression. | class | *connect-file-pulse-status* | high |
|`internal.kafka.reporter.id` | Group id the internal topic used by tasks and connector to report and monitor file progression | string | *-* | high |
|`internal.kafka.reporter.cluster.bootstrap.servers` | Reporter identifier to be used by tasks and connector to report and monitor file progression (must be unique per connector). | string | *-* | high |


## Cleaning completed files

The connector can be configured with a specific [FileCleanupPolicy](connect-file-pulse-api/src/main/java/io/streamthoughts/kafka/connect/filepulse/clean/FileCleanupPolicy.java) implementation.

The cleanup policy can be configured with the below connect property :

| Configuration |   Description |   Type    |   Default |   Importance  |
| --------------| --------------|-----------| --------- | ------------- |
|`fs.cleanup.policy.class` | The class used by tasks to read input files | class | *-* | high |


| Policy      | Description   | Example   |
| --------------| --------------|-----------|
| `io.streamthoughts.kafka.connect.filepulse.clean.DeleteCleanPolicy` | Deletes the file source file  |
| `io.streamthoughts.kafka.connect.filepulse.clean.LogCleanPolicy`  | Logs the absolute path of the source file |
| `io.streamthoughts.kafka.connect.filepulse.clean.MoveCleanPolicy` | Attempts to move atomically the source file into a configurable target directory |

## FAQ

**Could we deployed kafka-connect-file-pulse in distributed mode ?**

Connect File Pulse must be running locally to the machine hosting files to be ingested. It is recommend to deploy your connector in distributed mode. Multiple Kafka Connect workers can be deployed on the same machine and participating in the same cluster. The configured input directory will be scanned by the JVM running the SourceConnector. Then, all detected files will be scheduled amongs the tasks spread on your local cluster.

**Is kafka-connect-file-pulse fault-tolerant ?**

Connect File Pulse guarantees no data loss by leveraging Kafka Connect fault-tolerance capabilities.
Each task keeps a trace of the file offset of the last record written into Kafka. In case of a restart, tasks will continue where they stopped before crash.
Note, that some duplicates maybe written into Kafka.

**Is kafka-connect-file-pulse could be used in place of other solutions like Logstash ?**

Connect File Pulse has some features which are similar to the ones provided by Logstash [codecs](https://www.elastic.co/guide/en/logstash/current/codec-plugins.html)/[filters](https://www.elastic.co/guide/en/logstash/current/filter-plugins.html). Filters like GrokFilter are actually strongly inspired from Logstash. For example you can use it to parse non-structured data like application logs.

However, Connect File Pulse has not be originally designed to collect dynamic application log files.

## Contributions
Any contribution is welcome

## Licence
Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

[http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0)

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License