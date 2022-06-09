---
date: 2022-06-03
title: "Accessing Data and Metadata"
linkTitle: "Accessing Data and Metadata"
weight: 60
description: >
  Learn how to use the Simple Connect Expression Language (SCeL) for accessing and manipulating data.
---

Some filters (e.g : [AppendFilter](/kafka-connect-file-pulse/docs/developer-guide/filters/#appendfilter)) can be configured using the *Simple Connect Expression Language*.

*Simple Connect Expression Language* (ScEL for short) is an expression language that allows accessing and manipulating record fields and metadata.

The syntaxes to define an expression are of the form : `<expression string>` or `"{{ <expression string> }}"`.

## Expressions

ScEL supports the following capabilities :

* **Literal expressions**
* **Field Selector**
* **Nested Navigation**
* **String substitution**
* **Functions**

### Literal expressions

* String : `'Hello World'`
* Number : `42`
* Boolean: `True`
* Nullable: `null`

### Field Selector

The expression language can be used to easily select one field from the input record : 

`$.username`

### Nested Navigation

To navigate down a struct value, just use a period to indicate a nested field value: 

`$.address.city`

### String substitution

The expression language can be used to easily build a new string field that concatenate multiple ones: 

`The user {{ $.username }} is living in city {{ $.address.city }}`

### Function

The expression language provides built-in functions that can be used for easily transforming a field value:

`The user {{ $.username }} is living in city {{ uppercase($.address.city) }}`

Functions can also be nested to build more complex transformations.
For example, the below expression shows how to replace all whitespace characters after transforming a field's value into lowercase.

```
replace_all(lowercase($.field), '\\s', '-')
```

{{% alert title="Limitations" color="warning" %}}
Currently, FilePulse does not support user-defined functions (UDFs). So you cannot register your own functions to enrich the expression language.
{{% /alert %}}

### Dynamic Field Selector

String substitution can be used to dynamically select a field : 

The bellow example shows how to dynamically build a field selector by concatenating `$.` and 
the first element present in the array field `$.values`.

`{{ '$.'extract_array($.values, 0) }}`

## Scopes

In the previous section, we saw how to use the expression language to select a specific field was part of the record being processed.

In addition to that, _ScEL_ allows you to access additional fields through the use of _Scopes_.
Basically, a _scope_ defined the _root object_ on which a selector expression will be evaluated.

The syntax to define an expression with a _scope_ is of the form : "`$[<scope>].<selector expression string>`".

By default, if no _scope_ is defined in the expression, the scope `$value` is implicitly used.

ScEL supports a number of predefined _scopes_ that can be used for example :

- **To define the _topic_, the _key_, the _headers_, or the _timestamp_ for the record.**
- **To access to the metadata of the source file.**
- **To keep transient and contextual data between filters**.
- Etc.

| Scope        | Description                                                | Type                  |
|--------------|------------------------------------------------------------|-----------------------|
| `$headers`   | The record headers                                         | `map[string, object]` |
| `$key`       | The record key                                             | `string`              |
| `$metadata`  | The file metadata                                          | `struct`              |
| `$offset`    | The offset information of this record into the source file | `struct`              |
| `$system`    | The system environment variables and runtime properties    | `struct`              |
| `$timestamp` | The record timestamp                                       | `long`                |
| `$topic`     | The output topic                                           | `string`              |
| `$value`     | The record value                                           | `struct`              |
| `$variables` | The contextual filter-chain variables                      | `map[string, object]` |

{{% alert title="Access error" color="info" %}}
In case of failures an additional `$error` scope will be added to the current filter context (see : [Handling Failures](/kafka-connect-file-pulse/docs/developer-guide/handling-failures/))
{{% /alert %}}

### Record Headers

The scope `headers` allows defining the headers of the output record.

### Record key

The scope `key` allows defining the key of the output record. Only string key is currently supported.

### Source Metadata

The scope `metadata` allows read access to information about the file being processing.

#### Commons Metadata

| Predefined Fields (ScEL)        | Description                                                           | Type                  |
|---------------------------------|-----------------------------------------------------------------------|-----------------------|
| `$metadata.name`                | The URI of the source object.                                         | `string`              |
| `$metadata.uri`                 | The name of the source object.                                        | `string`              |
| `$metadata.contentLength`       | The content-length of the source object.                              | `string`              |
| `$metadata.lastModified`        | The creation date or the last modified date, whichever is the latest. | `string`              |
| `$metadata.contentDigest`       | The digest of the source object content.                              | `string`              |
| `$metadata.userDefinedMetadata` | The user-defined metadata.                                            | `Map[string, object]` |

The `userDefinedMetadata` object may contain additional information (i.e. properties) about the source object.

**Azure**

* `azure.blob.storage.account`
* `azure.blob.storage.blobUrl`
* `azure.blob.storage.creationTime`
* `azure.blob.storage.contentEncoding`
* `azure.blob.storage.contentType`

**AWS**

* `s3.object.summary.bucketName`
* `s3.object.summary.key`
* `s3.object.summary.etag`
* `s3.object.summary.storageClass`
* `s3.object.user.metadata.<METADATA>`  (optional)

**GCP**

* `gcs.blob.bucket`
* `gcs.blob.name`
* `gcs.blob.storageClass` (optional)
* `gcs.blob.contentEncodinge` (optional)
* `gcs.blob.contentType` (optional)
* `gcs.blob.createTime` (optional)
* `gcs.blob.ownerType` (optional)
* `gcs.blob.user.metadata.<METADATA>` (optional)

#### Local File Object

For files read from the local file system, the following additional metadata will be available.

| Predefined Fields (ScEL) | Description                  | Type     |
|--------------------------|------------------------------|----------|
| `$metadata.absolutePath` | The file absolute path       | `string` |
| `$metadata.inode`        | The file Unix inode          | `long`   |
| `$metadata.path`         | The file directory path      | `string` |


### Record Offset

The scope `offset` allows read access to information about the original position of the record into the source file.
The available fields depend on the configured FileInputRecord.

| Predefined Fields (ScEL) | Description                                   | Type   |
|--------------------------|-----------------------------------------------|--------|
| `$offset.timestamp`      | The creation time of the record (millisecond) | `long` |

Information only available if `RowFilterReader` is configured.

| Predefined Fields (ScEL) | Description                                           | Type   |
|--------------------------|-------------------------------------------------------|--------|
| `$offset.startPosition`  | The start position of the record into the source file | `long` |
| `$offset.endPosition`    | The end position of the record into the source file   | `long` |
| `$offset.size`           | The size in bytes                                     | `long` |
| `$offset.rows`           | The number of rows already read from the source file. | `long` |

Information only available if `BytesArrayInputReader` is configured.

| Predefined Fields (ScEL) | Description                                                                   | Type   |
|--------------------------|-------------------------------------------------------------------------------|--------|
| `$offset.startPosition`  | The start position of the record into the source file (always equals to 0)    | `long` |
| `$offset.endPosition`    | The end position of the record into the source file (equals to the file size) | `long` |

Information only available if `AvroFilterInputReader` is configured.

| Predefined Fields (ScEL) | Description                                       | Type   |
|--------------------------|---------------------------------------------------|--------|
| `$offset.blockStart`     | The start position of the current block           | `long` |
| `$offset.position`       | The position into the current block.              | `long` |
| `$offset.records`        | The number of record read into the current block. | `long` |

### System

The scope `system` allows accessing to the system environment variables and runtime properties.

| Predefined Fields (ScEL) | Description                        | Type                  |
|--------------------------|------------------------------------|-----------------------|
| `$system.env`            | The system environment variables.  | `map[string, string]` |
| `$system.props`          | The system environment properties. | `map[string, string]` |

### Timestamp

The scope `$timestamp` allows defining the timestamp of the output record.

### Topic

The scope `$topic` allows defining the target topic of the output record.

### Value

The scope `$value` allows defining the fields of the output record

### Variables

The scope `$variables` allows read/write access to a simple key-value map structure.
This scope can be used to share user-defined variables between [Processing Filters](/kafka-connect-file-pulse/docs/developer-guide/filters/).

{{% alert title="Warning" color="warning" %}}
Variables are not cached between records.
{{% /alert %}}

## Built-in Functions

ScEL supports a number of predefined functions that can be used to apply a single transformation on a field.

### Numeric functions

ScEL numeric functions are used primarily for numeric manipulation and/or mathematical calculations.

#### `CONVERTS`

| **Since**: **`-`**                                        |
|-----------------------------------------------------------|
| **Syntax** : `{{ converts(<field_expression>, <type>) }}` |
| **Returned type** : `ANY`                                 |

> Converts one type to another. The following casts are supported:

#### `GT`

| **Since**: **`2.4.0`**                                            |
|-------------------------------------------------------------------|
| **Syntax** : `{{ gt(<field_expression1>, <field_expression2>) }}` |
| **Returned type** : `BOOLEAN`                                     |

> Executes "*less than operation*" on two values and returns `TRUE` if the first value is less than the second value, `FALSE`, otherwise.

#### `LT`

| **Since**: **`2.4.0`**                                            |
|-------------------------------------------------------------------|
| **Syntax** : `{{ lt(<field_expression1>, <field_expression2>) }}` |
| **Returned type** : `BOOLEAN`                                     |

> Executes "*greater than operation*" on two values and returns `TRUE` if the first value is greater than the second value, `FALSE`, otherwise.

### Binary Functions

#### `AND`

| **Since**: **`2.4.0`**                                                        |
|-------------------------------------------------------------------------------|
| **Syntax** : `{{ and(<boolean_expression1>, <boolean_expression2>, [...]) }}` |
| **Returned type** : `BOOLEAN`                                                 |

> Checks if all of the given conditional expressions are `TRUE`.

#### `IF`

| **Since**: **`2.4.0`**                                                                                  |
|---------------------------------------------------------------------------------------------------------|
| **Syntax** : `{{ if(<boolean_expression>, <value_expression_if_true>,  <value_expression_if_false>) }}` |
| **Returned type** : `BOOLEAN`                                                                           |

> Evaluates the given boolean expression and returns one value if `TRUE` and another value if `FALSE`.

#### `NOT`

| **Since**: **`2.4.0`**                         |
|------------------------------------------------|
| **Syntax** : `{{ not(<boolean_expression>) }}` |
| **Returned type** : `BOOLEAN`                  |

> Reverses a boolean value.

#### `OR`

| **Since**: **`2.4.0`**                                                       |
|------------------------------------------------------------------------------|
| **Syntax** : `{{ or(<boolean_expression1>, <boolean_expression2>, [...]) }}` |
| **Returned type** : `BOOLEAN`                                                |

> Checks if at least one of the given conditional expressions is `TRUE`.

### Collection

#### `EXCTRACT_ARRAY`

| **Since**: **`-`**                                              |
|-----------------------------------------------------------------|
| **Syntax** : `{{ extract_array(<array_expression>, <index>) }}` |
| **Returned type** : `ANY`                                       |

> Returns the element at the specified position of the specified array. 

#### `LENGTH`

| **Since**: **`2.4.0`**                          |
|-------------------------------------------------|
| **Syntax** : `{{ length(<array_expression>) }}` |
| **Returned type** : `INTEGER`                   |

> Returns the number of elements into an array or the length of a string field

#### `CONTAINS`

| **Since**: **`-`**                                                    |
|-----------------------------------------------------------------------|
| **Syntax** : `{{ contains(<array_expression>, <value_expression>) }}` |
| **Returned type** : `BOOLEAN`                                         |

> Returns `TRUE` if an array contains a given value.

### Date and time

#### `TIMESTAMP_DIFF`

| **Since**: **`2.4.0`**                                                                    |
|-------------------------------------------------------------------------------------------|
| **Syntax** : `{{ timestamp_diff(unit, epoch_time_expression1, epoch_time_expression2) }}` |
| **Returned type** : `LONG`                                                                |

> Calculates the amount of time between two epoch times in seconds or milliseconds. For more information on `unit` see [ChronoUnit](https://docs.oracle.com/javase/8/docs/api/java/time/temporal/ChronoUnit.html).
 
#### `TO_TIMESTAMP`

| **Since**: **`2.4.0`**                                                             |
|------------------------------------------------------------------------------------|
| **Syntax** : `{{  to_timestamp(<datetime_expression>, <pattern>, [<timezone>]) }}` |
| **Returned type** : `LONG`                                                         |

> Parses a given string value and returns the epoch-time in milliseconds.

#### `UNIX_TIMESTAMP`

| **Since**: **`2.4.0`**                |
|---------------------------------------|
| **Syntax** : `{{ unix_timestamp() }}` |
| **Returned type** : `LONG`            |

> Returns the current time in milliseconds.

### Nulls

#### `IS_EMPTY`

| **Since**: **`2.4.0`**                             |
|----------------------------------------------------|
| **Syntax** : `{{ is_empty(<array_expression1>) }}` |
| **Returned type** : `BOOLEAN`                      |

> Returns `TRUE` if an array as no elements or a string field has no characters

#### `IS_NULL`

| **Since**: **`2.4.0`**                           |
|--------------------------------------------------|
| **Syntax** : `{{ is_null(<field_expression>) }}` |
| **Returned type** : `BOOLEAN`                    |

> Returns `TRUE` if a field's value is `NULL`.

#### `NLV`

| **Since**: **`-`**                                                 |
|--------------------------------------------------------------------|
| **Syntax** : `{{ nlv(<field_expression>, <default_expression>) }}` |
| **Returned type** : `Any`                                          |

> Sets a default value if a field's value is `NULL`

### Strings & Objects

#### `CONCAT`

| **Since**: **`-`**                                                        |
|---------------------------------------------------------------------------|
| **Syntax** : `{{ concat(<field_expression1>, <field_expression2, ...) }}` |
| **Returned type** : `STRING`                                              |

> Returns a `STRING` value consisting of the concatenation of two or more string expressions.

##### Examples

**Concatenate two fields**

* _Expression_:

  `{{ concat(world'hello','') }}`

* _Output (type = `STRING`)_:

  `helloworld`

#### `CONCAT_WS`

| **Since**: **`-`**                                                                                             |
|----------------------------------------------------------------------------------------------------------------|
| **Syntax** : `{{ concat_ws(<separator>, <prefix>, <suffix>, <field_expression1>, <field_expression2>, ...) }}` |
| **Returned type** : `STRING`                                                                                   |

> Returns a `STRING` value consisting of the concatenation of two or more string expressions, using the specified separator between each. 
  Optionally, the returned `STRING` may be prefixed and/or suffixed.

##### Examples

**Concatenate two fields**

* _Expression_:

  `{{ concat(' ', '', '!', 'hello','world') }}`

* _Output (type = `STRING`)_:

  `hello world!`

##### `HASH`

| **Since**: **`-`**                            |
|-----------------------------------------------|
| **Syntax** : `{{ hash(<field_expression>) }}` |
| **Returned type** : `STRING`                  |

> Returns the hashed of a given `STRING` expression, using murmur2 algorithm.

#### `EQUALS`

| **Since**: **`-`**                                                  |
|---------------------------------------------------------------------|
| **Syntax** : `{{ equals(<field_expression>, <value_expression>) }}` |
| **Returned type** : `BOOLEAN`                                       |

>  Returns `TRUE` if a `STRING` or number fields's value equals the specified value.

#### `ENDS_WITH`

| **Since**: **`-`**                                           |
|--------------------------------------------------------------|
| **Syntax** : `{{ ends_with(<field_expression>, <suffix>) }}` |
| **Returned type** : `BOOLEAN`                                |

> Returns `TRUE` if a string field's value end with the specified string suffix.

##### Examples

**Check whether a field ends with a given suffix**

* _Expression_:

  `{{ ends_with('thumbnail.png', '.png') }}`

* _Output (type = `BOOLEAN`)_:

  `true`

#### `EXISTS`

| **Since**: **`-`**                                        |
|-----------------------------------------------------------|
| **Syntax** : `{{ exists(<struct_expression>, <field>) }}` |
| **Returned type** : `BOOLEAN`                             |

> Returns `TRUE` if a `STRUCT` has the specified field.

#### `EXTRACT_STRUCT_FIELD`

| **Since**: **`2.7.0`**                                                 |
|------------------------------------------------------------------------|
| **Syntax** : `{{ extract_struct_field(<struct_expression>, <path>) }}` |
| **Returned type** : `ANY`                                              |

> Extracts the value at the specified field `path` from the `STRUCT` returned by the given `struct_expression`.
> If the requested `path` does not exist, the function returns `NULL`.

#### `FROM_BYTES`

| **Since**: **`2.7.0`**                                       |
|--------------------------------------------------------------|
| **Syntax** : `{{ from_bytes(struct_expression, '<path>') }}` |
| **Returned type** : `STRING`                                 |

> Converts a `BYTES` value to a `STRING` in the specified encoding type.
> The following list shows the supported encoding types: `hex`, `utf8`, `ascii` and `base64`.

#### `LOWERCASE`

| **Since**: **`-`**                                 |
|----------------------------------------------------|
| **Syntax** : `{{ lowercase(<field_expression>) }}` |
| **Returned type** : `STRING`                       |

> Converts all of the characters in a `STRING` value to lower case.

##### Examples

**Converts a field to lowercase**

* _Expression_:

  `{{ lowercase('Apache Kafka') }}`

* _Output (type = `STRING`)_

  `apache kafka`

#### `MATCHES`

| **Since**: **`-`**                                        |
|-----------------------------------------------------------|
| **Syntax** : `{{ matches(<field_expression>, <regex>) }}` |
| **Returned type** : `BOOLEAN`                             |

> Returns `TRUE` if a field's value match the specified regex.

#### `MD5`

| **Since**: **`-`**                           |
|----------------------------------------------|
| **Syntax** : `{{ md5(<field_expression>) }}` |
| **Returned type** : `STRING`                 |

> Returns the MD5 digest of `STRING` value.

#### `REPLACE_ALL`

| **Since**: **`-`**                                                           |
|------------------------------------------------------------------------------|
| **Syntax** : `{{ replace_all(<field_expression>, <regex>, <replacement>) }}` |
| **Returned type** : `STRING`                                                 |

> Replaces every subsequence of a `STRING` that matches the given pattern with the given replacement string.

#### `SPLIT`

| **Since**: **`-`**                                                 |
|--------------------------------------------------------------------|
| **Syntax** : `{{ split(<field_expression>, <regex>, [<limit>]) }}` |
| **Returned type** : `ARRAY<STRING>`                                |

> Splits a `STRING` value using the specified regex or character and returns the resulting array. 

#### `STARTS_WITH`

| **Since**: **`-`**                                             |
|----------------------------------------------------------------|
| **Syntax** : `{{ starts_with(<field_expression>, <suffix>) }}` |
| **Returned type** : `BOOLEAN`                                  |

> Returns `STRING` if a string field's value start with the specified string prefix.

##### Examples

**Check whether a field starts with a given prefix**

* _Expression_: 

  `{{ starts_with('fr_FR', 'fr') }}`

* _Output (type = `BOOLEAN`)_: 

  `true`


#### `TRIM`

| **Since**: **`-`**                            |
|-----------------------------------------------|
| **Syntax** : `{{ trim(<field_expression>) }}` |
| **Returned type** : `STRING`                  |

> Trims the spaces from the beginning and end of a string.

##### Examples

**Remove leading and tailing blank spaces from strings**

* _Expression_:

  `{{ trim('   FilePulse    ') }}`

* _Output (type = `STRING`):_

  `FilePulse`

#### `UPPERCASE`

| **Since**: **`-`**                                 |
|----------------------------------------------------|
| **Syntax** : `{{ uppercase(<field_expression>) }}` |
| **Returned type** : `STRING`                       |

> Converts all of the characters in a `STRING` value to upper case.

##### Examples

**Convert a field to uppercase**

* _Expression_:

  `{{ uppercase('Apache Kafka') }}`

* _Output_ (type = `STRING`):

  `APACHE KAFKA`

#### `UUID`

| **Since**: **`-`**           |
|------------------------------|
| **Syntax** : `{{ uuid() }}`  |
| **Returned type** : `STRING` |

> Returns a Universally Unique Identifier (UUID)

### URLs

#### `PARSE_URL`

| **Since**: **`2.7.0`**                                             |
|--------------------------------------------------------------------|
| **Syntax** : `{{ parse_url(<field_expression>, [<permissive>]) }}` |

> Parses a valid field-value URL/URI and return a struct consisting of all the components (fragment, host, path, port, query, scheme, userInfo).

##### Examples

**Parse a simple URL:**

* _Expression_: 

    `{{ parse_url('https://www.example.com') }}`

* _Output_ (type=`STRUCT`):

  ```json
  {"host":"www.example.com", "path":"","port":null,"scheme":"https", "fragment":null,"query":null, "userInfo": null}
  ```

**Parse a complex URL that includes a path, a port number, and user information:**

* _Expression_:

   `{{ parse_url('http://user:password@example.com:1234/index.html?user=1') }}`

* _Output_ (type=`STRUCT`):

  ```json
  {"host":"www.example.com", "path":"/index.html", "port":1234, "scheme":"http", "fragment":null, "query":"?user=1", "userInfo": "user:password"}
  ```

**Parse an email URL:**

* _Expression_:

    `{{ parse_url('mailto:abc@xyz.com') }}`

* _Output_ (type=`STRUCT`):

  ```json
  {"host":null, "path":"abc@xyz.com", "port":null, "scheme":"mailto", "fragment":null, "query":null, "userInfo":null}
  ```

**Parse an invalid URL that is missing the scheme.  missing _scheme_.**

Set the `permissive<>` parameter set to `true` to indicate that the function should return an object that contains the error message.

* _Expression_:

    `{{ parse_url('example.com', true) }}`

* _Output_ (type=`STRUCT`):

  ```json
  {"error":"Could not parse URL: scheme not specified"}
  ```