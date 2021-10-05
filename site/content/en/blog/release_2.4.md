---
title: "Connect FilePulse 2.4 is Released 🚀"
linkTitle: "Connect FilePulse 2.4 is Released"
date: 2021-10-04
description: ""
author: Florian Hussonnois ([@fhussonnois](https://twitter.com/fhussonnois))
---

I am pleased to announce the release of Connect FilePulse 2.4. This release brings new built-in expression functions, processing filters as well as some minor improvements and bug fixes.

## Simple Connect Expression Language

This release packs with new built-in functions to enrich the powerful expression language provided by connect FilePulse:

**Boolean Functions**

| Function         | Description |
| -----------------|-------------|
| `and`            | Checks if all of the given conditional expressions are `true`. |
| `gt`             | Executes "*greater than operation*" on two values and returns `true` if the first value is greater than the second value, `false`, otherwise. | 
| `if`             | Evaluates the given boolean expression and returns one value if `true` and another value if `false`. | 
| `lt`             | Executes "*less than operation*" on two values and returns `true` if the first value is less than the second value, `false`, otherwise. | 
| `not`            | Reverses a boolean value |
| `or`             | Checks if at least one of the given conditional expressions is `true`.  |

**Date and time Functions**

| Function         | Description |
| -----------------|-------------|
| `timestamp_diff` | Calculates the amount of time between two epoch times in seconds or milliseconds. For more information on `unit` see [ChronoUnit](https://docs.oracle.com/javase/8/docs/api/java/time/temporal/ChronoUnit.html). |
| `to_timestamp`   | Parses a given string value and returns the epoch-time in milliseconds.  |
| `unix_timestamp` | Returns the current time in milliseconds. |

These new functions allows for specifying more complex conditions on configured [*Processing Filters*](https://streamthoughts.github.io/kafka-connect-file-pulse/docs/developer-guide/filters/). 
For example, we can use them to remove all records retrieved from files that are older than 24 hours.

```properties
filters=DropTooLateFiles
filters.DropTooLateFiles.type=io.streamthoughts.kafka.connect.filepulse.filter.DropFilter
filters.DropTooLateFiles.if="{{ gt(timestamp_diff('HOURS', $metadata.lastModified, unix_timestamp()), 24) }}"
filters.DropTooLateFiles.invert=false
```

# XML Processing

XML is still widely used in legacy systems. To ease the integration of this data in Kafka, 
this release introduces two new *Processing Filters*: `XmlToStructFilter` and `XmlToJsonFilter` which can be used in addition to the existing `XMLFileInputReader`.

**XmlToStructFilter**

This _processing filter_ can be used to parse and convert an XML file that you read, for example, using the `LocalBytesArrayInputReader` into a `Struct` record.
This filter should be preferred to the `XMLFileInputReader` when you need to deal with invalid XML files.
 
For example, you may want to send invalid XML files into specific _Dead Letter Topic_.

```properties
filters=ParseXmlDocument
filters.ParseXmlDocument.type=io.streamthoughts.kafka.connect.filepulse.filter.XmlToStructFilter
filters.ParseXmlDocument.source=message
filters.ParseXmlDocument.xml.parser.validating.enabled=true
filters.ParseXmlDocument.xml.parser.namespace.aware.enabled=true
filters.ParseXmlDocument.xml.exclude.empty.elements=true
filters.ParseXmlDocument.xml.exclude.node.attributes=false
filters.ParseXmlDocument.xml.data.type.inference.enabled=true
filters.ParseXmlDocument.withOnFailure=SetToErrorTopic
filters.SetToErrorTopic.type=io.streamthoughts.kafka.connect.filepulse.filter.AppendFilter
filters.SetToErrorTopic.value=xml-parsing-DLQ
filters.SetToErrorTopic.field=$topic
```

**XmlToJsonFilter**

This _processing filter_ can be used to parse and convert an XML file that you read, for example, using the `LocalBytesArrayInputReader` into a JSON string record.

# Exception Context

When an exception occurs in the _processing filter chain_, Connect FilePulse allows you to access the context of the exception using the `$error` scope from the expression language.
In previous versions, only the exception message was available (e.g., using `$error.message`). Now, you can retrieve the exception stacktrace as well as the exception class name using:

* `$error.exceptionMessage` 
* `$error.exceptionStacktrace` 
* `$error.exceptionClassName` 

The below examples shows how to add the exception information to teh record headers.

```properties
...
filters.ParseXmlDocument.withOnFailure=SetToErrorTopic,AppendErrorMessageToHeader,AppendErrorStacktraceToHeader,AppendErrorClassNameToHeader
filters.AppendErrorMessageToHeader.type=io.streamthoughts.kafka.connect.filepulse.filter.AppendFilter
filters.AppendErrorMessageToHeader.field=$headers.errors.exception.message
filters.AppendErrorMessageToHeader.value={{ $error.exceptionMessage }}
filters.AppendErrorStacktraceToHeader.type=io.streamthoughts.kafka.connect.filepulse.filter.AppendFilter
filters.AppendErrorStacktraceToHeader.field=$headers.errors.exception.stacktrace
filters.AppendErrorStacktraceToHeader.value={{ $error.exceptionStacktrace }}
filters.AppendErrorClassNameToHeader.type=io.streamthoughts.kafka.connect.filepulse.filter.AppendFilter
filters.AppendErrorClassNameToHeader.field=$headers.errors.exception.message
filters.AppendErrorClassNameToHeader.value={{ $error.exceptionClassName }}
```

## Full Release Notes

Connect File Pulse 2.4 can be downloaded from the [GitHub Releases Page](https://github.com/streamthoughts/kafka-connect-file-pulse/releases/tag/v2.4.0). 

### Features
* [67683d5](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/67683d5) feat(expression): add built-in SCeL expression function NOT
* [7fea775](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/7fea775) feat(dataformat): add config to specify a prefix used to prepend XML attributes (#176)
* [4fc2cb9](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/4fc2cb9) feat(expression): add expression function TimestampDiff
* [9d72e47](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/9d72e47) feat(expression): add expression function ToTimestamp
* [0644cb9](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/0644cb9) feat(expressions): add built-in function 'gt' and 'lt' to ScEL
* [e4375c8](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/e4375c8) feat(expressions): add built-in function 'or' and 'and' to ScEL
* [28a6126](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/28a6126) feat(expressions): add built-in function 'if' to ScEL
* [4fe77fd](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/4fe77fd) feat(api): add access to error stacktrace in filter chain
* [b9c0a40](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/b9c0a40) feat(dataformat): add new config prop to exclude node attributes in namespaces (#175)
* [8f648c8](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/8f648c8) feat(dataformat): add new config props to exclude all XML attributes (#174)
* [355b6e4](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/355b6e4) feat(expression): add UnixTimestamp expression function
* [5a62f03](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/5a62f03) feat(filters): add new XmlToStructFilter
* [9cad2fa](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/9cad2fa) feat(filters): add new simple XmlToJsonFilter
* [0e29ce2](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/0e29ce2) feat(plugin): add capability to merge schemas deriving from records

### Improvements & Bugfixes
* [165a908](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/165a908) refactor(expressions): allow functions to not evaluate all expression args
* [4e9f84d](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/4e9f84d) fix(expressions): fix equals SCeL expression should support null argument (#187)
* [7bdc787](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/7bdc787) fix(filesystems): fix regression on AmazonS3Client configuration (#184)
* [d76bac0](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/d76bac0) fix(plugin): refactor InMemoryFileObjectStateBackingStore to use an LRU cache (#183)
* [50200f7](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/50200f7) fix(plugin): fix resources must not be closed while files are not committed
* [17e9efb](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/17e9efb) fix(plugin): fix regression cleanup object files should not be rescheduled (#178)
* [e2f74b2](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/e2f74b2) fix(api): fix schemas should be merged per target topic
* [03bab9a](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/03bab9a) fix(api): enhance mapping to connect schema to handle duplicate schema
* [760d98b](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/760d98b) fix(filters): XmlToJson should support bytes input
* [99c374f](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/99c374f) fix(api): fix schema behavior on array merge

## Sub-Tasks
* [e9cd483](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/e9cd483) fix(build): normalize artefact-ids
* [2b8d260](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/2b8d260) refactor(filters): relocate json packages
* [4d13731](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/4d13731) refactor(filters): cleanup classes
* [f3179a7](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/f3179a7) refactor(expression): refactor expression function api
* [bf3fc31](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/bf3fc31) refactor(expression): reorganize packages for built-in functions

### Docs
* [643469f](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/643469f) site(docs): update documentations
* [be29aae](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/be29aae) docs(site): add new function descriptions
* [2a9a119](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/2a9a119) docs(site): fix missing config property
* [7533d2f](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/7533d2f) docs(site): improve installation guide
* [71a9ebe](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/71a9ebe) docs(site): add doc for defining schema

If you enjoyed reading this post, check out Connect FilePulse at GitHub and give us a ⭐!
