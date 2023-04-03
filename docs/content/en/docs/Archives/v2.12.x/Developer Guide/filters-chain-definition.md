---
date: 2022-06-08
title: "Filter Chain Definition"
linkTitle: "Filter Chain Definition"
weight: 50
description: >
  Learn how to define complex pipelines to transform and structure your data before integration into Kafka.
---

The connector can be configured to apply complex transformations on messages before they are written to Kafka.

## Configuration

A [filter](../filters) chain can be specified in the connector configuration.

 * `filters` - List of aliases for the filter, specifying the order in which the filters will be applied.
 * `filters.$alias.type` - Fully qualified class name for the filter.
 * `filters.$alias.$filterSpecificConf` - Configuration properties for the filter

For example, let's parse a standard application logs file written with log4j using the build-in filters :

```properties
filters=GroupMultilineException, ExtractFirstLine, ParseLog4jLog

filters.GroupMultilineException.type=io.streamthoughts.kafka.connect.filepulse.filter.MultiRowFilter
filters.GroupMultilineException.negate=false
filters.GroupMultilineException.pattern="^[\\t]"

filters.ExtractFirstLine.type=io.streamthoughts.kafka.connect.filepulse.filter.AppendFilter
filters.ExtractFirstLine.field=$.logmessage
filters.ExtractFirstLine.values={{ extract_array($.message, 0) }

filters.ParseLog4jLog.type=io.streamthoughts.kafka.connect.filepulse.filter.impl.GrokFilter
filters.ParseLog4jLog.match="%{TIMESTAMP_ISO8601:logdate} %{LOGLEVEL:loglevel} %{GREEDYDATA:thread} %{GREEDYDATA:logmessage}"
filters.ParseLog4jLog.source=log
filters.ParseLog4jLog.overwrite=logmessage
```

## List of Processing Filters Available

These filters are available for use with Kafka Connect File Pulse:

| Filter                                              | Description                                                                              | Since    |
|-----------------------------------------------------|------------------------------------------------------------------------------------------|----------|
| [AppendFilter](../filters#appendfilter)             | Appends one or more values to an existing or non-existing array field                    |          |
| [ConvertFilter](../filters#convertfilter)           | Converts a message field's value to a specific type                                      |          |
| [CSVFilter](../filters#csvfilter)                   | Parses a message field's value containing columns delimited by a character into a struct | `v2.7.0` |
| [DateFilter](../filters#datefilter)                 | Converts a field's value containing a date to a unix epoch time                          |          |
| [DelimitedRowFilter](../filters#delimitedrowfilter) | Parses a message field's value containing columns delimited by a separator into a struct |          |
| [DropFilter](../filters#dropfilter)                 | Drops messages satisfying a specific condition without throwing exception.               |          |
| [ExcludeFilter](../filters#excludefilter)           | Excludes one or more fields from the input record.                                       | `v1.4.0` |
| [ExplodeFilter](../filters#explodefilter)           | Explodes an array or list field into separate records.                                   | `v1.4.0` |
| [FailFilter](../filters#failfilter)                 | Throws an exception when a message satisfy a specific condition                          |          |
| [GrokFilter](../filters#grokfilter)                 | Parses an unstructured message field's value to a struct by combining Grok patterns      |          |
| [GroupRowFilter](../filters#grouprowfilter)         | Regroups multiple following messages into a single message by composing a grouping key   |          |
| [JoinFilter](../filters#joinfilter)                 | Joins values of an array field with a specified separator                                |          |
| [JSONFilter](../filters#jsonfilter)                 | Unmarshallings a JSON message field's value to a complex struct                          |          |
| [MoveFilter](../filters#movefilter)                 | Moves an existing record field's value to a specific target path                         | `v1.5.0` |
| [MultiRowFilter](../filters#multirowfilter)         | Combines following message lines into single one by combining patterns                   |          |
| [NullValueFilter](../filters#nullvaluefilter)       | Combines following message lines into single one by combining patterns                   | `v2.3.0` |
| [RenameFilter](../filters#renamefilter)             | Renames a message field                                                                  |          |
| [SplitFilter](../filters#splitfilter)               | Splits a message field's value to array                                                  |          |
| [XmlToJsonFilter](../filters#xmltojsonfilter)       | Parses an XML record-field and convert it to a JSON string                               | `v2.4.0` |
| [XmlToStructFilter](../filters#xmltostructfilter)   | Parses an XML record-field into STRUCT                                                   | `v2.4.0` |

## Difference between Kafka Connect Single Message Transforms (SMT) functionality

Filters can be compared to Kafka Connect built-in [Transformers](https://kafka.apache.org/documentation/#connect_transforms).
However, filters allow more complex pipelines to be built for structuring file data.
For example, they can be used to split one input message to multiple messages or to temporarily buffer consecutive messages in order to regroup them by fields or a pattern.