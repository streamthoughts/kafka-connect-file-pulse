---
date: 2022-06-03
title: "Processing Filters"
linkTitle: "Processing Filters"
weight: 80
description: >
  The list of available transformation filters.
---

These filters are available for use with Kafka Connect File Pulse:

| Filter                                    | Description                                                                              | Since    |
|-------------------------------------------|------------------------------------------------------------------------------------------|----------|
| [AppendFilter](#appendfilter)             | Appends one or more values to an existing or non-existing array field                    |          |
| [ConvertFilter](#convertfilter)           | Converts a message field's value to a specific type                                      |          |
| [CSVFilter](#csvfilter)                   | Parses a message field's value containing columns delimited by a character into a struct | `v2.7.0` |
| [DateFilter](#datefilter)                 | Converts a field's value containing a date to a unix epoch time                          |          |
| [DelimitedRowFilter](#delimitedrowfilter) | Parses a message field's value containing columns delimited by a regex into a struct     |          |
| [DropFilter](#dropfilter)                 | Drops messages satisfying a specific condition without throwing exception.               |          |
| [ExcludeFilter](#excludefilter)           | Excludes one or more fields from the input record.                                       | `v1.4.0` |
| [ExplodeFilter](#explodefilter)           | Explodes an array or list field into separate records.                                   | `v1.4.0` |
| [FailFilter](#failfilter)                 | Throws an exception when a message satisfy a specific condition                          |          |
| [GrokFilter](#grokfilter)                 | Parses an unstructured message field's value to a struct by combining Grok patterns      |          |
| [GroupRowFilter](#grouprowfilter)         | Regroups multiple following messages into a single message by composing a grouping key   |          |
| [JoinFilter](#joinfilter)                 | Joins values of an array field with a specified separator                                |          |
| [JSONFilter](#jsonfilter)                 | Unmarshalling a JSON message field's value to a complex struct                           |          |
| [MoveFilter](#movefilter)                 | Moves an existing record field's value to a specific target path                         | `v1.5.0` |
| [MultiRowFilter](#multirowfilter)         | Combines following message lines into single one by combining patterns                   |          |
| [NullValueFilter](#nullvaluefilter)       | Combines following message lines into single one by combining patterns                   | `v2.3.0` |
| [RenameFilter](#renamefilter)             | Renames a message field                                                                  |          |
| [SplitFilter](#splitfilter)               | Splits a message field's value to array                                                  |          |
| [XmlToJsonFilter](#xmltojsonfilter)       | Parses an XML record-field and convert it to a JSON string                               | `v2.4.0` |
| [XmlToStructFilter](#xmltostructfilter)   | Parses an XML record-field into STRUCT                                                   | `v2.4.0` |

## AppendFilter

The following provides usage information for : `io.streamthoughts.kafka.connect.filepulse.filter.AppendFilter`

The `AppendFilter` is probably one of the most important processing filters to know.
It allows you to manipulate a source record by easily adding or replacing a field with a constant
value or a value extracted from another existing field
using  [ScEL](/kafka-connect-file-pulse/docs/developer-guide/accessing-data-and-metadata/).

### Configuration

| Configuration | Description                           | Type                                                                                                   | Default | Importance |
|---------------|---------------------------------------|--------------------------------------------------------------------------------------------------------|---------|------------|
| `field`       | The name of the field to be added     | string ([ScEL supported](/kafka-connect-file-pulse/docs/developer-guide/accessing-data-and-metadata/)) | *-*     | high       |
| `value`       | The value of the field to be added    | string ([ScEL supported](/kafka-connect-file-pulse/docs/developer-guide/accessing-data-and-metadata/)) | *-*     | high       |
| `overwrite`   | Is existing field should be overwrite | boolean                                                                                                | *false* | high       |

### Examples

The following examples shows how to use the `AppendFilter` to concat two values from the array field named `values`
using
a [substitution expression](/kafka-connect-file-pulse/docs/developer-guide/accessing-data-and-metadata/#string-substitution)
.
The concat value is then added to the field named `result`.

**Configuration**

```properties
filters=SubstituteFilter
filters.SubstituteFilter.type=io.streamthoughts.kafka.connect.filepulse.filter.AppendFilter
filters.SubstituteFilter.field="$.result"
filters.SubstituteFilter.value="{{ extract_array($.values,0) }}-{{ extract_array($.values,1) }}"
```

**Input**

```json
{
  "record": {
    "value": [
      "Hello",
      "World"
    ]
  }
} 
```

**Output**

```json
{
  "record": {
    "value": [
      "Hello",
      "World"
    ],
    "result": "Hello-World"
  }
}
```

In the previous example, we used the simple property expression `result` to indicate the target field to which our value
is added.
We have actually omitted
the [expression scope](/kafka-connect-file-pulse/docs/developer-guide/accessing-data-and-metadata/#scopes) `$value`.
By default, if no scope is defined in an expression, the scope `$value` is implicitly applied.
Hence, we could have used the fully expression `$value.result` which is similar to the simplified expression `result`.

But, you can perfectly use another expression scope. For example, you can leverage the `AppendFilter` to dynamically
resolve the record-key or the output topic based on the record data.

The following configuration show how to use the `$topic` scope :

```properties
filters.SubstituteFilter.field="$topic"
filters.SubstituteFilter.value="my-topic-{{ lowercase(extract_array($.values,0)) }}"
```

**Input**

```json
{
  "record": {
    "value": [
      "Hello",
      "World"
    ]
  }
} 
```

**Output**

```json
{
  "context": {
    "topic": "my-topic-hello"
  },
  "record": {
    "value": [
      "Hello",
      "World"
    ]
  }
} 
```

Finally, the `AppendFilter` can also accept a substitution expression for the property field.
This allows to dynamically determine the name of the field to be added.

The following examples show how to use a property expression to get the named of the field from a

```properties
filters.SubstituteFilter.field="$.target"
filters.SubstituteFilter.value="{{ extract_array($.values, 0) }}-{{ extract_array($.values,1) }}"
```

**Input**

```json
{
  "record": {
    "target": "result",
    "value": [
      "Hello",
      "World"
    ]
  }
}
```

**Output**

```json
{
  "record": {
    "target": "result",
    "value": [
      "Hello",
      "World"
    ],
    "result": "Hello-World"
  }
}
```

## ConvertFilter

The following provides usage information for : `io.streamthoughts.kafka.connect.filepulse.filter.ConvertFilter`

The `ConvertFilter` can be used to convert a field's value into a specific type.

### Configuration

| Configuration   | Description                                                                                                                                      | Type    | Default | Importance |
|-----------------|--------------------------------------------------------------------------------------------------------------------------------------------------|---------|---------|------------|
| `field`         | The field to convert (dot notation is supported)                                                                                                 | string  | *-*     | high       |
| `to`            | The type to which the field must be converted                                                                                                    | string  | *,*     | high       |
| `default`       | The default value to apply if the field cannot be converted                                                                                      | string  | *,*     | medium     |
| `ignoreMissing` | If true and field does not exist the filter will be apply successfully without modifying the data. If field is null the schema will be modified. | boolean | *true*  | high       |

Supported types are :

* `SHORT`
* `INTEGER`
* `LONG`
* `FLOAT`
* `DOUBLE`
* `BOOLEAN`
* `STRING`
* `ARRAY`
* `BYTES`

### Examples

The following example shows how to convert a a field's value containing the string `yes` into a boolean.

**Configuration**

```properties
filters.BooleanConverter.field="target"
filters.BooleanConverter.to="BOOLEAN"
```

**Input**

```json
{
  "record": {
    "target": "yes"
  }
}
```

**Output**

```json
{
  "record": {
    "target": true
  }
}
```

## CsvFilter

The following provides usage information for: `io.streamthoughts.kafka.connect.filepulse.filter.CSVFilter`

The `CsvFilter` can be used to parse a message field's value containing columns delimited by a character into a struct

### Configuration

| Configuration                | Description                                                                                                                                        | Type    | Default | Importance |
|------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------|---------|---------|------------|
| `separator`                  | Sets the delimiter to use for separating entries.                                                                                                  | string  | *,*     | High       |
| `ignore.quotations`          | Sets the ignore quotations mode - if true, quotations are ignored.                                                                                 | boolean | *false* | Medium     |
| `escape.char`                | Sets the character to use for escaping a separator or quote.                                                                                       | string  | *\\*    | Medium     |
| `ignore.leading.whitespace`  | Sets the ignore leading whitespace setting - if true, white space in front of a quote in a field is ignored.                                       | boolean | *true*  | Medium     |
| `quote.char`                 | Sets the character to use for quoted elements.                                                                                                     | string  | *"*     | Medium     |
| `strict.quotes`              | Sets the strict quotes setting - if true, characters outside the quotes are ignored.                                                               | boolean | *false* | Medium     |
| `trim.column`                | Remove the leading and trailing whitespaces from all columns.                                                                                      | boolean | *false* | Low        |
| `duplicate.columns.as.array` | Treat duplicate columns as an array. If false and a record contains duplicate columns an exception will be thrown.                                 | boolean | *false* | High       |
| `extract.column.name`        | Define the field from which the schema should be detected (all columns will be of type 'string')                                                   | string  |         | High       |
| `auto.generate.column.names` | Define whether column names should autogenerated or not (column names will of the form 'column1, column2')                                         | string  |         | High       |
| `columns`                    | The list of comma-separated column names in order they appear in each row. Columns must be in the form of: COL1_NAME:COL1_TYPE;COL2_NAME:COL2_TYPE | string  |         | High       |

### Examples

The following example shows the use of the `CSVFilter` to split the `message` field using a comma (`,`) as a separator character.
The name of each column is extracted from the fields `headers`.

```properties
filters=ParseCSVLine
filters.ParseCSVLine.type="io.streamthoughts.kafka.connect.filepulse.filter.CSVFilter"
filters.ParseCSVLine.extract.column.name="headers"
filters.ParseCSVLine.separator=","
filters.ParseCSVLine.trim.column="true"
```

## DateFilter

The following provides usage information for : `io.streamthoughts.kafka.connect.filepulse.filter.DateFilter`

The `DateFilter` converts a field's value containing a date to a unix epoch time.

### Configuration

| Configuration | Description                           | Type                                                                                                  | Default | Importance |
|---------------|---------------------------------------|-------------------------------------------------------------------------------------------------------|---------|------------|
| `field`       | The field to get the date from .      | string([ScEL supported](/kafka-connect-file-pulse/docs/developer-guide/accessing-data-and-metadata/)) | *-*     | high       |
| `target`      | The target field.                     | string([ScEL supported](/kafka-connect-file-pulse/docs/developer-guide/accessing-data-and-metadata/)) | *-*     | high       |
| `timezone`    | The timezone to use for parsing date. | string                                                                                                | *UTC*   | high       |
| `locale`      | The locale to use for parsing date.   | string                                                                                                | *en_EN* | high       |
| `formats`     | List of the expected date formats.    | list                                                                                                  | *-*     | high       |

### Examples

```properties
filters=ParseISODate
filters.ParseISODate.type=io.streamthoughts.kafka.connect.filepulse.filter.DateFilter
filters.ParseISODate.field="$.date"
filters.ParseISODate.target="$.timestamp"
filters.ParseISODate.formats="yyyy-MM-dd'T'HH:mm:ss"
```

**Input**

```json
{
  "record": {
    "date": "2001-07-04T12:08:56"
  }
}
```

**Output**

```json
{
  "record": {
    "date": "2001-07-04T12:08:56",
    "timestamp": 994248536000
  }
}
```

## DelimitedRowFilter

The following provides usage information for : `io.streamthoughts.kafka.connect.filepulse.filter.DelimitedRowFilter`.

The `DelimitedRowFilter` can be used to parse and stream delimited row files (i.e CSV) into Kafka.
Each row is parsed and published into a configured topic as a single Kafka data.

### Configuration

| Configuration       | Description                                                                                                         | Type    | Default | Importance |
|---------------------|---------------------------------------------------------------------------------------------------------------------|---------|---------|------------|
| `separator`         | The character used as a delimiter/separator between each value                                                      | string  | *;*     | high       |
| `trimColumn`        | Remove the leading and trailing whitespaces from all columns.                                                       | boolean | *false* | low        |
| `extractColumnName` | Define the field from which the schema should be detected (all columns will be of type 'string')                    | string  |         | high       |
| `columns`           | The list of comma-separated column names in order they appear in each row. columns must be in the form of NAME:TYPE | string  |         | high       |

### Examples

The following example shows the use of the `DelimitedRowFilter` to split the `message` field using `|` as a separator
character.
The name of each column is extracted from the fields `headers`.

```properties
filters=ParseDelimitedRow
filters.ParseDelimitedRow.extractColumnNam="headers"
filters.ParseDelimitedRow.separator="\\|"
filters.ParseDelimitedRow.trimColumn="true"
filters.ParseDelimitedRow.type="io.streamthoughts.kafka.connect.filepulse.filter.DelimitedRowFilter"
```

{{% alert title="Important" color="info" %}}
Under the hood, the `DelimitedRowFilter` will use
the [`String#split`](https://docs.oracle.com/javase/9/docs/api/java/lang/String.html#split-java.lang.String-) method to
parse the input line. This
method accepts a regex as argument then any special character must be escaped.
{{% /alert %}}

## DropFilter

The following provides usage information for : `io.streamthoughts.kafka.connect.filepulse.filter.DropFilter`.

The `DropFilter` can be used to prevent some messages (i.e records) to be written into Kafka.

### Configuration

| Configuration | Description                                                | Type                                                                                                  | Default | Importance |
|---------------|------------------------------------------------------------|-------------------------------------------------------------------------------------------------------|---------|------------|
| `if`          | Condition to apply the filter on the current record.       | string [ScEL supported](/kafka-connect-file-pulse/docs/developer-guide/accessing-data-and-metadata/)) | *-*     | high       |
| `invert`      | Invert the boolean value return from the filter condition. | boolean                                                                                               | *false* | medium     |

For more information about `if` property, see : [Conditional execution](conditional-execution).

### Examples

The following example shows the usage of **DropFilter** to only keep records with a field `level` containing to `ERROR`.

```properties
filters=Drop
filters.Drop.type=io.streamthoughts.kafka.connect.filepulse.filter.DropFilter
filters.Drop.if={{ equals($.level, 'ERROR') }}
filters.Drop.invert=true
```

## ExcludeFilter

The following provides usage information for : `io.streamthoughts.kafka.connect.filepulse.filter.ExcludeFilter`.

The `ExcludeFilter` can be used to exclude one or more fields from the input record.

### Configuration

| Configuration | Description                                        | Type | Default | Importance |
|---------------|----------------------------------------------------|------|---------|------------|
| `fields`      | The comma-separated list of field names to exclude | list | **      | high       |

### Examples

The following example shows the usage of **ExplodeFilter**.

```properties
filters=Exclude
filters.Exclude.type=io.streamthoughts.kafka.connect.filepulse.filter.ExcludeFilter
filters.Exclude.fields=message
```

**Input**

```json
{
  "record": {
    "message": "{\"name\":\"pulse\"}",
    "name": "pulse"
  }
}
```

**Output**

```json
{
  "record": {
    "name": "pulse"
  }
}
```

## ExplodeFilter

The following provides usage information for : `io.streamthoughts.kafka.connect.filepulse.filter.ExplodeFilter`.

The `ExplodeFilter` can be used to explode an array or list field into separate records.

### Configuration

| Configuration | Description                                  | Type   | Default   | Importance |
|---------------|----------------------------------------------|--------|-----------|------------|
| `source`      | The input field on which to apply the filter | string | *message* | medium     |

### Examples

The following example shows the usage of **ExplodeFilter**.

```properties
filters=Explode
filters.Explode.type=io.streamthoughts.kafka.connect.filepulse.filter.ExplodeFilter
filters.Explode.source=measurements
```

**Input (single record)**

```json
{
  "record": {
    "id": "captor-0001",
    "date": "2020-08-06T17:00:00",
    "measurements": [
      38,
      40,
      42,
      37
    ]
  }
}
```

**Output (multiple records)**

```json
{
  "record": {
    "id": "captor-0001",
    "date": "2020-08-06T17:00:00",
    "measurements": 38
  }
}
{
  "record": {
    "id": "captor-0001",
    "date": "2020-08-06T17:00:00",
    "measurements": 40
  }
}
{
  "record": {
    "id": "captor-0001",
    "date": "2020-08-06T17:00:00",
    "measurements": 42
  }
}
{
  "record": {
    "id": "captor-0001",
    "date": "2020-08-06T17:00:00",
    "measurements": 37
  }
}
```

## FailFilter

The following provides usage information for : `io.streamthoughts.kafka.connect.filepulse.filter.FailFilter`.

The fail filter can be used to throw an exception with a provided error message.
For example, this can be useful to stop processing a file when a non-conform record is read.

### Configuration

| Configuration | Description                                                                                                                             | Type    | Default | Importance |
|---------------|-----------------------------------------------------------------------------------------------------------------------------------------|---------|---------|------------|
| `if`          | Condition to apply the filter on the current record.                                                                                    | string  | *-*     | high       |
| `invert`      | Invert the boolean value return from the filter condition.                                                                              | boolean | *false* | medium     |
| `message`     | The error message thrown by the filter. ([ScEL supported](/kafka-connect-file-pulse/docs/developer-guide/accessing-data-and-metadata/)) | string  | *-*     | high       |

### Examples

The following example shows the usage of **FailFilter** to stop processing a file when a field is equals to `null`.

```properties
filters=Fail
filters.Fail.type=io.streamthoughts.kafka.connect.filepulse.filter.FailFilter
filters.Fail.if={{ is_null($.user_id) }}
filters.Fail.message=Invalid row, user_id is missing : {{ $value }}
```

## GrokFilter

The following provides usage information for : `io.streamthoughts.kafka.connect.filepulse.filter.GrokFilter`.

The `GrokFilter` allows you to parse unstructured data like applications logs to extract structured and meaningful data
fields.

The `GrokFilter`is based on: https://github.com/streamthoughts/kafka-connect-transform-grok

### Configuration

| Configuration        | Description                                   | Type    | Default   | Importance |
|----------------------|-----------------------------------------------|---------|-----------|------------|
| `namedCapturesOnly`  | If true, only store named captures from grok. | boolean | *true*    | high       |
| `pattern`            | The Grok pattern to match.                    | string  | *-*       | high       |
| `overwrite`          | The fields to overwrite.                      | list    | medium    |
| `patternDefinitions` | Custom pattern definitions.                   | list    | *-*       | low        |
| `patternsDir`        | List of user-defined pattern directories      | string  | *-*       | low        |
| `source`             | The input field on which to apply the filter  | string  | *message* | medium     |

### Examples

The following example shows the usage of **GrokFilter** to parse and extract fields from application log message.

```properties
filters=ParseLog4jLog
filters.ParseLog4jLog.pattern="%{TIMESTAMP_ISO8601:logdate} %{LOGLEVEL:loglevel} %{GREEDYDATA:message}"
filters.ParseLog4jLog.overwrite="message"
filters.ParseLog4jLog.source="message"
filters.ParseLog4jLog.type="io.streamthoughts.kafka.connect.filepulse.filter.GrokFilter"
filters.ParseLog4jLog.ignoreFailure="true"
```

## GroupRowFilter

The following provides usage information for : `io.streamthoughts.kafka.connect.filepulse.filter.GroupRowFilter`.

### Configuration

| Configuration          | Description                                            | Type    | Default   | Importance |
|------------------------|--------------------------------------------------------|---------|-----------|------------|
| `fields`               | List of fields used to regroup records                 | list    | high      |
| `max.buffered.records` | The maximum number of records to group (default : -1). | integer | *-1*      | high       |
| `target`               | The target array field to put the grouped field        | integer | *records* | high       |

### Examples

```properties
```

## JoinFilter

The following provides usage information for : `io.streamthoughts.kafka.connect.filepulse.filter.JoinFilter`.

### Configuration

| Configuration | Description                                  | Type                                                                                                  | Default | Importance |
|---------------|----------------------------------------------|-------------------------------------------------------------------------------------------------------|---------|------------|
| `field`       | The field to get the date from               | string([ScEL supported](/kafka-connect-file-pulse/docs/developer-guide/accessing-data-and-metadata/)) | *-*     | high       |
| `target`      | The target field                             | string([ScEL supported](/kafka-connect-file-pulse/docs/developer-guide/accessing-data-and-metadata/)) | *-*     | high       |
| `separator`   | The separator used for joining array values. | string                                                                                                | *,*     | high       |

### Examples

## JSONFilter

The following provides usage information for : `io.streamthoughts.kafka.connect.filepulse.filter.JSONFilter`.

The `JSONFilter` parses an input json field.

### Configuration

| Configuration   | Description                                                                                    | Type    | Default   | Importance |
|-----------------|------------------------------------------------------------------------------------------------|---------|-----------|------------|
| `overwrite`     | The fields to overwrite                                                                        | list    | *-*       | medium     |
| `source`        | The input field on which to apply the filter                                                   | string  | *message* | medium     |
| `target`        | The target field to put the parsed JSON data                                                   | string  | *-*       | high       |
| `charset`       | The charset to be used for reading the source field (if source if of type `BYTES`              | string  | *UTF-8*   | medium     |
| `explode.array` | A boolean that specifies whether to explode arrays into separate records                       | boolean | *false*   | medium     |
| `merge`         | boolean that specifies whether to merge the JSON object into the top level of the input record | boolean | *false*   | medium     |

### Examples

```properties
filters=MyJsonFilter
filters.MyJsonFilter.type=io.streamthoughts.kafka.connect.filepulse.filter.JSONFilter
filters.MyJsonFilter.source=message
filters.MyJsonFilter.target=payload
```

## MultiRowFilter

The following provides usage information for : `io.streamthoughts.kafka.connect.filepulse.filter.MultiRowFilter`.

The `MultiRowFilter` joins multiple lines into a single Struct using a regex pattern.

### Configuration

| Configuration        | Description                                    | Type    | Default | Importance |
|----------------------|------------------------------------------------|---------|---------|------------|
| `negate`             | Negate the regexp pattern (if not matched).    | boolean | *-*     | medium     |
| `pattern`            | The pattern to match multiline                 | string  | *-*     | high       |
| `patternDefinitions` | Custom pattern definitions.                    | list    | *-*     | low        |
| `patternsDir`        | List of user-defined pattern directories       | string  | *-*     | low        |
| `separator`          | The character to be used to concat multi lines | string  | "\\n"   | high       |

## MoveFilter

The following provides usage information for : `io.streamthoughts.kafka.connect.filepulse.filter.MoveFilter`.

The `MoveFilter` moves an existing record field's value to a specific target path.

### Configuration

| Configuration | Description                    | Type   | Default | Importance |
|---------------|--------------------------------|--------|---------|------------|
| `source`      | The path of the field to move" | string | *-*     | high       |
| `target`      | The path to move the field     | string | *-*     | high       |

### Examples

The following example shows the usage of the `MoveFilter`.

```properties
filters=MyMoveFilter
filters.MyMoveFilter.type=io.streamthoughts.kafka.connect.filepulse.filter.MoveFilter
filters.MyMoveFilter.source=field.child
filters.MyMoveFilter.target=moved
```

**Input**

```json
{
  "record": {
    "field": {
      "child": "foo"
    }
  }
}
```

**Output**

```json
{
  "record": {
    "moved": "foo"
  }
}
```

## NullValueFilter

The following provides usage information for : `io.streamthoughts.kafka.connect.filepulse.filter.NullValueFilter`.

The `NullValueFilter` is used to empty a record-value to null.

### Example

```properties
filters=NullValueIfDeleteOp
filters.NullValueIfDeleteOp.type=io.streamthoughts.kafka.connect.filepulse.filter.NullValueFilter
filters.NullValueIfDeleteOp.if={{ equals($value.op, 'DELETE') }}
```

## RenameFilter

The following provides usage information for : `io.streamthoughts.kafka.connect.filepulse.filter.RenameFilter`.

The `RenameFilter` is used to rename a specified field.

### Configuration

| Configuration   | Description                                                                                                                                      | Type    | Default | Importance |
|-----------------|--------------------------------------------------------------------------------------------------------------------------------------------------|---------|---------|------------|
| `field`         | The field to rename                                                                                                                              | string  | *-*     | high       |
| `target`        | The target name                                                                                                                                  | string  | *-*     | high       |
| `ignoreMissing` | If true and field does not exist the filter will be apply successfully without modifying the data. If field is null the schema will be modified. | boolean | *true*  | high       |

### Examples

```properties
filters=RenameInputField
filters.RenameInputField.type=io.streamthoughts.kafka.connect.filepulse.filter.RenameFilter
filters.RenameInputField.field=input
filters.RenameInputField.target=renamed
```

**Input**

```json
{
  "record": {
    "input": "foo"
  }
}
```

**Output**

```json
{
  "record": {
    "renamed": "foo"
  }
}
```

## SplitFilter

The following provides usage information for : `io.streamthoughts.kafka.connect.filepulse.filter.SplitFilter`.

The `SplitFilter` splits a field's value of type string into an array by using a specific separator.

### Configuration

| Configuration | Description                                                       | Type   | Default | Importance |
|---------------|-------------------------------------------------------------------|--------|---------|------------|
| `split`       | The comma-separated list of fields to split                       | string | *-*     | high       |
| `separator`   | The separator used for splitting a message field's value to array | string | *,*     | high       |
| `target`      | The target field to put the parsed JSON data                      | string | *-*     | high       |

### Example

**Configuration**

```properties
filters=SplitInputField
filters.SplitInputField.type=io.streamthoughts.kafka.connect.filepulse.filter.SplitFilter
filters.SplitInputField.split=input
filters.SplitInputField.separator=,
```

**Input**

```json
{
  "record": {
    "input": "val0,val1,val2"
  }
}
```

**Output**

```json
{
  "record": {
    "input": "val0,val1,val2",
    "output": [
      "val0",
      "val1",
      "val2"
    ]
  }
}
```

## XmlToJsonFilter

The following provides usage information for : `io.streamthoughts.kafka.connect.filepulse.filter.XmlToJsonFilter`.

The `XmlToJsonFilter` parses and converts an XML record-field it to a JSON string.
This is filter is based on the `org.json.XML` library.

### Configuration

| Configuration             | Description                                                                                                                                                           | Type    | Default     | Importance |
|---------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------|-------------|------------|
| `source`                  | The input field on which to apply the filter.                                                                                                                         | string  | `"message"` | high       |
| `source.charset`          | The charset to be used for reading the source.                                                                                                                        | string  | `"UTF-8"`   | high       |
| `xml.parser.keep.strings` | When parsing the XML into JSON, specifies if values should be kept as strings (true), or if they should try to be guessed into JSON values (numeric, boolean, string) | boolean | `false`     | high       |
| `xml.parser.cDataTagName` | The name of the key in a JSON Object that indicates a CDATA section.                                                                                                  | string  | `"value"`   | high       |

### Example

**Configuration**

```properties
filters=XmlToJson
filters.XmlToJson.type=io.streamthoughts.kafka.connect.filepulse.filter.XmlToJsonFilter
filters.XmlToJson.xml.parser.keep.strings=false
filters.XmlToJson.xml.parser.cDataTagName=data
```

## XmlToStructFilter

The following provides usage information for : `io.streamthoughts.kafka.connect.filepulse.filter.XmlToStructFilter`.

The `XmlToStructFilter` parses an XML record-field into STRUCT

### Configuration

| Configuration                                 | Since | Description                                                                                                                           | Type      | Default     | Importance |
|-----------------------------------------------|-------|---------------------------------------------------------------------------------------------------------------------------------------|-----------|-------------|------------|
| `source`                                      |       | The input field on which to apply the filter.                                                                                         | string    | `"message"` | High       |
| `xml.force.array.on.fields`                   |       | The comma-separated list of fields for which an array-type must be forced                                                             | `List`    |             | High       |                                                
| `xml.parser.validating.enabled`               |       | Specifies that the parser will validate documents as they are parsed.                                                                 | `boolean` | `false`     | Low        |
| `xml.parser.namespace.aware.enabled`          |       | Specifies that the XML parser will provide support for XML namespaces.                                                                | `boolean` | `false`     | Low        |
| `xml.exclude.empty.elements`                  |       | Specifies that the reader should exclude element having no field.                                                                     | `boolean` | `false`     | Low        |
| `xml.exclude.node.attributes`                 |       | Specifies that the reader should exclude all node attributes.                                                                         | `boolean` | `false`     | Low        |
| `xml.exclude.node.attributes.in.namespaces`   |       | Specifies that the reader should only exclude node attributes in the defined list of namespaces.                                      | `List`    | `false`     | Low        |
| `xml.data.type.inference.enabled`             |       | Specifies that the reader should try to infer the type of data nodes.                                                                 | `boolean` | `false`     | High       |
| `xml.attribute.prefix`                        |       | If set, the name of attributes will be prepended with the specified prefix when they are added to a record.                           | `string`  | `""`        | Low        |
| `xml.content.field.name`                      | 2.5.0 | Specifies the name to be used for naming the field that will contain the value of a TextNode element having attributes.               | `string`  | `value`     | Low        |
| `xml.field.name.characters.regex.pattern`     | 2.5.0 | Specifies the regex pattern to use for matching the characters in XML element name to replace when converting a document to a struct. | `string`  | `[.\-]'     | Low        |
| `xml.field.name.character.string.replacement` | 2.5.0 | Specifies the replacement string to be used when converting a document to a struct.                                                   | `string`  | `_`         | Low        |
| `xml.force.content.field.for.paths`           | 2.5.0 | The comma-separated list of field for which a content-field must be forced.                                                           | `List`    |             | Low        |
### Example

**Configuration**

```properties
filters=XmlToStruct
filters.ParseXmlDocument.type=io.streamthoughts.kafka.connect.filepulse.filter.XmlToStructFilter
filters.ParseXmlDocument.source=message
filters.ParseXmlDocument.xml.parser.validating.enabled=true
filters.ParseXmlDocument.xml.parser.namespace.aware.enabled=true
filters.ParseXmlDocument.xml.exclude.empty.elements=true
filters.ParseXmlDocument.xml.data.type.inference.enabled=true
```