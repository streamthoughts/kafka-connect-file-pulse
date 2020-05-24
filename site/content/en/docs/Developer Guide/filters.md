---
date: 2020-05-24
title: "Processing Filters"
linkTitle: "Processing Filters"
weight: 80
description: >
  The list of available transformation filters.
---

These filters are available for use with Kafka Connect File Pulse:

| Filter | Description |
|---     | --- |
| [AppendFilter](#appendfilter) | Appends one or more values to an existing or non-existing array field  |
| [ConvertFilter](#convertfilter)  | Converts a message field's value to a specific type |
| [DateFilter](#datefilter)  | Converts a field's value containing a date to a unix epoch time |
| [DelimitedRowFilter](#delimitedrowfilter)  | Parses a message field's value containing columns delimited by a separator into a struct |
| [DropFilter](#dropfilter)  | Drops messages satisfying a specific condition without throwing exception |
| [FailFilter](#failfilter)  | Throws an exception when a message satisfy a specific condition |
| [GrokFilter](#grokfilter)  | Parses an unstructured message field's value to a struct by combining Grok patterns |
| [GroupRowFilter](#grouprowfilter)  | Regroups multiple following messages into a single message by composing a grouping key|
| [JoinFilter](#joinfilter)  | Joins values of an array field with a specified separator |
| [JSONFilter](#jsonfilter)  | Unmarshallings a JSON message field's value to a complex struct |
| [MultiRowFilter](#multirowfilter)  | Combines following message lines into single one by combining patterns |
| [RenameFilter](#renamefilter)  | Renames a message field |
| [SplitFilter](#splitfilter)  | Splits a message field's value to array |

## AppendFilter

The following provides usage information for : `io.streamthoughts.kafka.connect.filepulse.filter.AppendFilter`

### Configuration

The 'Append' filter is probably one of the most important processing filters to know. 
It allows you to manipulate a source record by easily adding or replacing a field with a constant 
value or a value extracted from another existing field using  [ScEL](/kafka-connect-file-pulse/docs/developer-guide/accessing-data-and-metadata/).

| Configuration |   Description |   Type    |   Default |   Importance  |
| --------------| --------------|-----------| --------- | ------------- |
| `field` | The name of the field to be added    | string ([ScEL supported](/kafka-connect-file-pulse/docs/developer-guide/accessing-data-and-metadata/)) | *-* | high |
| `value` | The value of the field to be added    | string ([ScEL supported](/kafka-connect-file-pulse/docs/developer-guide/accessing-data-and-metadata/)) | *-* | high |
| `overwrite` | Is existing field should be overwrite    | boolean | *false* | high |

### Examples

The following examples shows how to use the `AppendFilter` to concat two values from the array field named `values`
using a [substitution expression](/kafka-connect-file-pulse/docs/developer-guide/accessing-data-and-metadata/#string-substitution). 
The concat value is then added to the a field named `result`.

**Configuration**
```properties
filters.SubstituteFilter.field="result"
filters.SubstituteFilter.value="{{ extract_array(values,0) }}-{{ extract_array(values,1) }}"
```

**Input**
```json
{
    "record" :{
      "value": ["Hello", "World"]
    }
} 
```
**Output**
```json
{
    "record" :{
      "value": ["Hello", "World"],
      "result" : "Hello-World"
    }
}
```

In the previous example, we used the simple property expression `result` to indicate the target field to which our value is added. 
We have actually omitted the [expression scope](/kafka-connect-file-pulse/docs/developer-guide/accessing-data-and-metadata/#scopes) `$value`.
By default, if no scope is defined in an expression, the scope `$value` is implicitly applied.
Hence, we could have used the fully expression `$value.result` which is similar to the simplified expression `result`.

But, you can perfectly used another expression scope. For example, you can leverage the `AppendFilter` to dynamically
resolve the record-key or the output topic based on the record data.

The following configuration show how to use the `$topic` scope : 

```properties
filters.SubstituteFilter.field="$topic"
filters.SubstituteFilter.value="my-topic-{{ lowercase(extract_array(values,0) }}"
```
**Input**
```json
{
  "record" : {
    "value": ["Hello", "World"]
  }
} 
```

**Output**
```json
{
  "context": { "topic" : "my-topic-hello" },
  "record" :{
    "value": ["Hello", "World"]
  }
} 
```

Finally the `AppendFilter` can also accept an substitution expression for the property field. 
This allows to dynamically determine the name of the field to be added.

The following examples show how to use a property expression to get the named of the field from a

```properties
filters.SubstituteFilter.field="{{ target }}"
filters.SubstituteFilter.value="{{ extract_array(values,0) }}-{{ extract_array(values,1) }}"
```

**Input**
```json
{
  "record" : {
    "target": "result",
    "value": ["Hello", "World"]
  }
}
```

**Output**
```json
{
  "record" : {
    "target": "result",
    "value": ["Hello", "World"],
    "result" : "Hello-World"
  }
}
```

## ConvertFilter

The following provides usage information for : `io.streamthoughts.kafka.connect.filepulse.filter.ConvertFilter`

### Configuration

| Configuration |   Description |   Type    |   Default |   Importance  |
| --------------| --------------|-----------| --------- | ------------- |
| `field` | The field to convert    | string | *-* | high |
| `type` | The type field must be converted to  | string | *,* | high |
| `ignoreMissing` | If true and field does not exist the filter will be apply successfully without modifying the data. If field is null the schema will be modified. | boolean | *true* | high |

### Examples

```properties
filters.SubstituteFilter.extractColumnNam="headers"
filters.SubstituteFilter.separator="\\|"
filters.SubstituteFilter.trimColumn="true"
```

## DateFilter

The following provides usage information for : `io.streamthoughts.kafka.connect.filepulse.filter.DateFilter`

### Configuration

| Configuration |   Description |   Type    |   Default |   Importance  |
| --------------| --------------|-----------| --------- | ------------- |
| `field` | The field to get the date from .   | string([ScEL supported](/kafka-connect-file-pulse/docs/developer-guide/accessing-data-and-metadata/)) | *-* | high |
| `target` | The target field.    | string([ScEL supported](/kafka-connect-file-pulse/docs/developer-guide/accessing-data-and-metadata/)) | *-* | high |
| `timezone` | The timezone to use for parsing date.  | string | *UTC* | high |
| `locale` | The locale to use for parsing date. | string | *en_EN* | high |
| `format` | List of the expected date formats. | list | *-* | high |

### Examples

```properties
```

## DelimitedRowFilter

The following provides usage information for : `io.streamthoughts.kafka.connect.filepulse.filter.DelimitedRowFilter`

The delimited-row filter can be used to parse and stream delimited row files (i.e CSV) into Kafka.
Each row is parsed and published into a configured topic as a single Kafka data.

### Configuration

| Configuration |   Description |   Type    |   Default |   Importance  |
| --------------| --------------|-----------| --------- | ------------- |
|`separator` | The character used as a delimiter/separator between each value | string |*;* |  high |
|`trimColumn` | Remove the leading and trailing whitespaces from all columns. |  boolean | *false* | low |
|`extractColumnName` | Define the field from which the schema should be detected (all columns will be of type 'string') | string | | high |
|`autoGenerateColumnNames` | Define whether column names should autogenerated or not (column names will of the form 'column1, column2') | *true* | boolean | high |
|`columns` | The list of comma-separated column names in order they appear in each row. columns must be in the form of NAME:TYPE | string | | high |

### Examples

The following example shows the use of the `DelimitedRowFilter` to split the `message` field using `|` as a separator character.
The name of each column is extracted from the fields `headers`.

```properties
filters.ParseDelimitedRow.extractColumnNam="headers"
filters.ParseDelimitedRow.separator="\\|"
filters.ParseDelimitedRow.trimColumn="true"
filters.ParseDelimitedRow.type="io.streamthoughts.kafka.connect.filepulse.filter.DelimitedRowFilter"
```

{{% alert title="Important" color="info" %}}
Under the hood, the `DelimitedRowFilter` will use the [`String#split`](https://docs.oracle.com/javase/9/docs/api/java/lang/String.html#split-java.lang.String-) method to parse the input line. This 
method accepts a regex as argument then any special character must be escaped.
{{% /alert %}}


## DropFilter

The following provides usage information for : `io.streamthoughts.kafka.connect.filepulse.filter.DropFilter`

The drop filter can be used to prevent some messages (i.n records) to be written into Kafka.

### Configuration

| Configuration |   Description |   Type    |   Default |   Importance  |
| --------------| --------------|-----------| --------- | ------------- |
|`if` | Condition to apply the filter on the current record. | string [ScEL supported](/kafka-connect-file-pulse/docs/developer-guide/accessing-data-and-metadata/)) |*-* |  high |
|`invert` | Invert the boolean value return from the filter condition. |  boolean | *false* | medium |

For more information about `if` property, see : [Conditional execution](conditional-execution).

### Examples

The following example shows the usage of **DropFilter** to only keep records with a field `level` containing to `ERROR`.

```properties
filters=Drop
filters.Drop.type=io.streamthoughts.kafka.connect.filepulse.filter.DropFilter
filters.Drop.if={{ equals(level, ERROR) }}
filters.Drop.invert=true
```

## FailFilter

The following provides usage information for : `io.streamthoughts.kafka.connect.filepulse.filter.FailFilter`

The fail filter can be used to throw an exception with a provided error message.
For example, this can be useful to stop processing a file when a non-conform record is read.

### Configuration

| Configuration |   Description |   Type    |   Default |   Importance  |
| --------------| --------------|-----------| --------- | ------------- |
|`if` | Condition to apply the filter on the current record. | string |*-* |  high |
|`invert` | Invert the boolean value return from the filter condition. |  boolean | *false* | medium |
|`message` | The error message thrown by the filter. ([ScEL supported](/kafka-connect-file-pulse/docs/developer-guide/accessing-data-and-metadata/)) |  string | *-* | high |

### Examples

The following example shows the usage of **FailFilter** to stop processing a file when a field is equals to `null`.

```properties
filters=Fail
filters.Fail.type=io.streamthoughts.kafka.connect.filepulse.filter.FailFilter
filters.Fail.if={{ is_null(user_id) }}
filters.Fail.message=Invalid row, user_id is missing : {{ $value }}
```

## GrokFilter

The following provides usage information for : `io.streamthoughts.kafka.connect.filepulse.filter.GrokFilter`

The grok filter allows you to parse unstructured data like applications logs to extract structured and meaningful data fields.

**Regular Expressions**
Grok are built on top of on regular expressions, so you can use any regular expressions as well to define your own patterns.

Internally, FilePulse uses the regular expression library is [Joni](https://github.com/jruby/joni)
, a Java port of [Oniguruma](https://github.com/kkos/oniguruma) regexp library.

### Configuration

| Configuration |   Description |   Type    |   Default |   Importance  |
| --------------| --------------|-----------| --------- | ------------- |
| `namedCapturesOnly` | If true, only store named captures from grok. | boolean | *true* | high |
| `matches` | The Grok pattern to match. | string | *-* | high |
| `overwrite` | The fields to overwrite.    | list | medium |
| `patternDefinitions` | Custom pattern definitions. | list | *-* | low |
| `patternsDir` | List of user-defined pattern directories | string | *-* | low |
| `source` | The input field on which to apply the filter  | string | *message* | medium |

### Examples

The following example shows the usage of **GrokFilter** to parse and extract fields from application log message.

```properties
filters.ParseLog4jLog.match="%{TIMESTAMP_ISO8601:logdate} %{LOGLEVEL:loglevel} %{GREEDYDATA:message}"
filters.ParseLog4jLog.overwrite="message"
filters.ParseLog4jLog.source="message"
filters.ParseLog4jLog.type="io.streamthoughts.kafka.connect.filepulse.filter.GrokFilter"
filters.ParseLog4jLog.ignoreFailure="true"
```

## GroupRowFilter

The following provides usage information for : `io.streamthoughts.kafka.connect.filepulse.filter.GroupRowFilter`

### Configuration

| Configuration |   Description |   Type    |   Default |   Importance  |
| --------------| --------------|-----------| --------- | ------------- |
| `fields` | List of fields used to regroup records | list | high |
| `max.buffered.records` | The maximum number of records to group (default : -1).| integer | *-1* | high |
| `target` | The target array field to put the grouped field | integer | *records* | high |

### Examples

```properties
```

## JoinFilter

The following provides usage information for : `io.streamthoughts.kafka.connect.filepulse.filter.JoinFilter`

### Configuration

| Configuration |   Description |   Type    |   Default |   Importance  |
| --------------| --------------|-----------| --------- | ------------- |
| `field` | The field to get the date from .   | string([ScEL supported](/kafka-connect-file-pulse/docs/developer-guide/accessing-data-and-metadata/)) | *-* | high |
| `target` | The target field.    | string([ScEL supported](/kafka-connect-file-pulse/docs/developer-guide/accessing-data-and-metadata/)) | *-* | high |
| `separator` | The separator used for joining array values.   | string | *,* | high |

### Examples

## JSONFilter

The following provides usage information for : `io.streamthoughts.kafka.connect.filepulse.filter.JSONFilter`


The JSON filter parses an input json field.

### Configuration

| Configuration |   Description |   Type    |   Default |   Importance  |
| --------------| --------------|-----------| --------- | ------------- |
| `overwrite` | The fields to overwrite.    | list | *-* | medium |
| `source` | The input field on which to apply the filter  | string | *message* | medium |
| `target` | he target field to put the parsed JSON data  | string | *-* | high |

### Examples

```properties
filters=MyJsonFilter
filters.MyJsonFilter.type=io.streamthoughts.kafka.connect.filepulse.filter.JSONFilter
filters.MyJsonFilter.source=message
filters.MyJsonFilter.target=payload
```

## MultiRowFilter

The following provides usage information for : `io.streamthoughts.kafka.connect.filepulse.filter.MultiRowFilter`


The multirow filter joins multiple lines into a single Struct using a regex pattern.

### Configuration

| Configuration |   Description |   Type    |   Default |   Importance  |
| --------------| --------------|-----------| --------- | ------------- |
| `negate` | Negate the regexp pattern (if not matched)."   | boolean | *-* | medium |
| `pattern` | The pattern to match multiline  | string | *-* | high |
| `patternDefinitions` | Custom pattern definitions. | list | *-* | low |
| `patternsDir` | List of user-defined pattern directories | string | *-* | low |
| `separator` | The character to be used to concat multi lines  | string | "\\n" | high |

### Examples

The following example shows the usage of the `MultiRowFilter` to join Java exception message with its stacktrace. 

```properties
filters=StackTraceMultiRowFilter
filters.StackTraceMultiRowFilter.type=io.streamthoughts.kafka.connect.filepulse.filter.MultiRowFilter
filters.StackTraceMultiRowFilter.negate=false
filters.StackTraceMultiRowFilter.pattern=^[\t]
```

## RenameFilter

The following provides usage information for : `io.streamthoughts.kafka.connect.filepulse.filter.RenameFilter`

### Configuration

| Configuration |   Description |   Type    |   Default |   Importance  |
| --------------| --------------|-----------| --------- | ------------- |
| `field` | The field to rename | string | *-* | high |
| `target` | The target name | string | *-* | high |
| `ignoreMissing` | If true and field does not exist the filter will be apply successfully without modifying the data. If field is null the schema will be modified. | boolean | *true* | high |

### Examples

```properties
```

## SplitFilter

The following provides usage information for : `io.streamthoughts.kafka.connect.filepulse.filter.SplitFilter`

### Configuration

| Configuration |   Description |   Type    |   Default |   Importance  |
| --------------| --------------|-----------| --------- | ------------- |
| `split` | Split a message field's value to array    | string | *-* | high |
| `separator` | The separator used for splitting a message field's value to array  | string | *,* | high |
| `target` | he target field to put the parsed JSON data  | string | *-* | high |