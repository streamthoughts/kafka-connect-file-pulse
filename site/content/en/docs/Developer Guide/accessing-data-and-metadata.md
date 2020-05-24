---
date: 2020-05-21
title: "Accessing Data and Metadata"
linkTitle: "Accessing Data and Metadata"
weight: 60
description: >
  The commons configuration for Connect File Pulse.
---

Some filters (e.g : [AppendFilter](#appendfilter)) can be configured using *Simple Connect Expression Language*.

*Simple Connect Expression Language* (ScEL for short) is an expression language based on regex that allows quick access and manipulating record fields and metadata.

The syntax to define an expression is of the form : "`{{ <expression string> }}`".

{{% alert title="Note" color="info" %}}
In some situation double brackets can be omitted if the expression is used to write a value into a target field.
{{% /alert %}}

ScEL supports the following capabilities :

* **Field Selector**
* **Nested Navigation**
* **String substitution**
* **Functions**

## Field Selector

The expression language can be used to easily select one field from the input record : 

"`{{ username }}`"

## Nested Navigation

To navigate down a struct value, just use a period to indicate a nested field value : 

"`{{ address.city }}`"

## String substitution

The expression language can be used to easily build a new string field that concatenate multiple ones : 

"`{{ <expression one> }}-{{ <expression two>}}`"

## Built-in Functions

ScEL supports a number of predefined functions that can be used to apply a single transformation on a field.

| Function       | Description   | Syntax   |
| ---------------| --------------|-----------|
| `contains`     | Returns `true` if an array field's value contains the specified value  | `{{ contains(array, value) }}` |
| `converts`     | Converts a field'value into the specified type | `{{ converts(field, INTEGER) }}` |
| `ends_with`    | Returns `true` if an a string field's value end with the specified string suffix | `{{ ends_with(field, suffix) }}` |
| `equals`       | Returns `true` if an a string or number fields's value equals the specified value | `{{ equals(field, value) }}` |
| `exists`       | Returns `true` if an the specified field exists | `{{ ends_with(field, value) }}` |
| `extract_array`| Returns the element at the specified position of the specified array | `{{extract_array(array, 0) }}` |
| `is_null`      | Returns `true` if a field's value is null | `{{ is_null(field) }}` |
| `length`       | Returns the number of elements into an array of the length of an string field | `{{ length(array) }}` |
| `lowercase`    | Converts all of the characters in a string field's value to lower case | `{{ lowercase(field) }}` |
| `matches`      | Returns `true` if a field's value match the specified regex | `{{ matches(field, regex) }}` |
| `nlv`          | Sets a default value if a field's value is null | `{{ length(array) }}` |
| `replace_all ` | Replaces every subsequence of the field's value that matches the given pattern with the given replacement string. | `{{ replace_all(field, regex, replacement) }}` |
| `starts_with`  | Returns `true` if an a string field's value start with the specified string prefix | `{{ starts_with(field, prefix) }}` |
| `trim`         | Trims the spaces from the beginning and end of a string.  | `{{ trim(field) }}` |
| `uppercase`    | Converts all of the characters in a string field's value to upper case  | `{{ uppercase(field) }}` |


In addition, ScEL supports nested functions. 

For example, the following expression is used to replace all whitespace characters after transforming our field's value into lowercase.

```
{{ replace_all(lowercase(field), \\s, -)}}
```

{{% alert title="Limitation" color="warning" %}}
Currently, FilePulse does not support user-defined functions (UDFs). So you cannot register your own functions to enrich the expression language.
{{% /alert %}}


## Scopes


In previous section, we have shown how to use the expression language to select a specific field.
The selected field was part of our the current record being processed.

Actually, ScEL allows you to get access to additional fields through the used of scopes. 
Basically, a scope defined the root object on which a selector expression must evaluated.

The syntax to define an expression with a scope is of the form : "`{{ $<scope>.<selector expression string> }}`".

By default, if no scope is defined in the expression, the scope `$value` is implicitly used.

ScEL supports a number of predefined scopes that can be used for example :

 - **To override the output topic.**
 - **To define record the key to be used.**
 - **To get access to the source file metadata.**
 - Etc.

| Scope | Description | Type |
|--- | --- |--- |
| `{{ $headers }}` | The record headers  | - |
| `{{ $key }}` | The record key | `string` |
| `{{ $metadata }}` | The file metadata  | `struct` |
| `{{ $offset }}` | The offset information of this record into the source file  | `struct` |
| `{{ $system }}` | The system environment variables and runtime properties | `struct` |
| `{{ $timestamp }}` | The record timestamp  | `long` |
| `{{ $topic }}` | The output topic | `string` |
| `{{ $value }}` | The record value| `struct` |
| `{{ $variables }}` | The contextual filter-chain variables| `map[string, object]` |

Note, that in case of failures more fields are added to the current filter context (see : [Handling Failures](./handling-failures)

### Record Headers

The scope `headers` allows to defined the headers of the output record.

### Record key

The scope `key` allows to defined the key of the output record. Only string key is currently supported.

### Source Metadata

The scope `metadata` allows read access to information about the file being processing.

| Predefined Fields (ScEL) | Description | Type |
|--- | --- |--- |
| `{{ $metadata.name }}` | The file name  | `string` |
| `{{ $metadata.path }}` | The file directory path | `string` |
| `{{ $metadata.absolutePath }}` | The file absolute path | `string` |
| `{{ $metadata.hash }}` | The file CRC32 hash | `int` |
| `{{ $metadata.lastModified }}` | The file last modified time.  | `long` |
| `{{ $metadata.size }}` | The file size  | `long` |
| `{{ $metadata.inode }}` | The file Unix inode  | `long` |

## Record Offset

The scope `offset` allows read access to information about the original position of the record into the source file.
The available fields depend of the configured FileInputRecord.

| Predefined Fields (ScEL) | Description | Type |
|--- | --- |--- |
| `{{ $offset.timestamp }}` | The creation time of the record (millisecond)  | `long` |

Information only available if `RowFilterReader` is configured.

| Predefined Fields (ScEL) | Description | Type |
|--- | --- |--- |
| `{{ $offset.startPosition }}` | The start position of the record into the source file  | `long` |
| `{{ $offset.endPosition }}` | The end position of the record into the source file  | `long` |
| `{{ $offset.size }}` | The size in bytes  | `long` |
| `{{ $offset.row }}` | The row number of the record into the source | `long` |

Information only available if `BytesArrayInputReader` is configured.

| Predefined Fields (ScEL) | Description | Type |
|--- | --- |--- |
| `{{ $offset.startPosition }}` | The start position of the record into the source file (always equals to 0)  | `long` |
| `{{ $offset.endPosition }}` | The end position of the record into the source file (equals to the file size)  | `long` |

Information only available if `AvroFilterInputReader` is configured.

| Predefined Fields (ScEL) | Description | Type |
|--- | --- |--- |
| `{{ $offset.blockStart }}` | The start position of the current block  | `long` |
| `{{ $offset.position }}` | The position into the current block.  | `long` |
| `{{ $offset.records }}` | The number of record read into the current block.  | `long` |

## System

The scope `system` allows read access to system environment variables and runtime properties.

| Predefined Fields (ScEL) | Description | Type |
|--- | --- |--- |
| `{{ $system.env }}` | The system environment variables.  | `map[string, string]` |
| `{{ $system.props }}` | The system environment properties. | `map[string, string]` |

## Timestamp

The scope `timestamp` allows to defined the timestamp of the output record.

## Topic

The scope `topic` allows to defined the target topic of the output record.

## Value

The scope `value` allows to defined the fields of the output record

## Variables

The scope `variables` allows read/write access to a simple key-value map structure.
This scope can be used to share user-defined variables between filters.

Note : variables are not cached between records.