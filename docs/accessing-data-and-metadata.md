# Accessing Data and Metadata

Some filter properties (like : [AppendFilter](#appendfilter)) can be configured using *Simple Connect Expression Language*.

*Simple Connect Expression Language* (ScEL for short) is an expression language based on regex that allow quick access and manipulating record fields and metadata.

The syntax to define an expression is of the form : "`{% raw %}{{ <expression string> }}{% endraw %}`".

Note : In some situation double brackets can be omitted if the expression is used to write a value into a target field.

ScEL supports the following functionality :

* Field Selector
* Nested Navigation
* String substitution
* Functions

## Field Selector

The expression language can be used to easily select one field from the input record : "`{% raw %}{{ username }}{% endraw %}`."

## Nested Navigation

To navigate down a struct value, just use a period to indicate a nested field value : "`{% raw %}{{ address.city }}{% endraw %}`."

## String substitution

The expression language can be used to easily build a new string field that concatenate multiple ones : "`{% raw %}{{ <expression one> }}-{{ <expression two>}}{% endraw %}`."

## Functions

ScEL supports a number of predefined functions that can be used to apply a single transformation on a field.

| Function       | Description   | Syntax   |
| ---------------| --------------|-----------|
| `contains`     | Returns `true` if an array field's value contains the specified value  | `{% raw %}{{ contains(array, value) }}{% endraw %}` |
| `converts`     | Converts a field'value into the specified type | `{% raw %}{{ converts(field, INTEGER) }}{% endraw %}` |
| `ends_with`    | Returns `true` if an a string field's value end with the specified string suffix | `{% raw %}{{ ends_with(field, suffix) }}{% endraw %}` |
| `equals`       | Returns `true` if an a string or number fields's value equals the specified value | `{% raw %}{{ equals(field, value) }}{% endraw %}` |
| `exists`       | Returns `true` if an the specified field exists | `{% raw %}{{ ends_with(field, value) }}{% endraw %}` |
| `extract_array`| Returns the element at the specified position of the specified array | `{% raw %}{{extract_array(array, 0) }}{% endraw %}` |
| `is_null`      | Returns `true` if a field's value is null | `{% raw %}{{ is_null(field) }}{% endraw %}` |
| `length`       | Returns the number of elements into an array of the length of an string field | `{% raw %}{{ length(array) }}{% endraw %}` |
| `lowercase`    | Converts all of the characters in a string field's value to lower case | `{% raw %}{{ lowercase(field) }}{% endraw %}` |
| `matches`      | Returns `true` if a field's value match the specified regex | `{% raw %}{{ matches(field, regex) }}{% endraw %}` |
| `nlv`          | Sets a default value if a field's value is null | `{% raw %}{{ length(array) }}{% endraw %}` |
| `replace_all ` | Replaces every subsequence of the field's value that matches the given pattern with the given replacement string. | `{% raw %}{{ replace_all(field, regex, replacement) }}{% endraw %}` |
| `starts_with`  | Returns `true` if an a string field's value start with the specified string prefix | `{% raw %}{{ starts_with(field, prefix) }}{% endraw %}` |
| `trim`         | Trims the spaces from the beginning and end of a string.  | `{% raw %}{{ trim(field) }}{% endraw %}` |
| `uppercase`    | Converts all of the characters in a string field's value to upper case  | `{% raw %}{{ uppercase(field) }}{% endraw %}` |


ScEL supports nested functions. For example this expression replace all whitespace characters after transforming our field's value into lowercase.

```
{% raw %}{{ replace_all(lowercase(field), \\s, -)}}{% endraw %}
```

**Limitations** :

* Currently, this is not possible to register user-defined functions (UDFs).

## Scopes


In previous section, we have demonstrated how to use the expression language to select a specific field. The selected field was part of our the current record being processed.

Actually, ScEL allows you to get access to additional fields through the used of scopes. Basically, a scope defined the root object on  which a selector expression must evaluated.

The syntax to define an expression  with a scope is of the form : "`{% raw %}{{ $scope.<selector expression string> }}{% endraw %}`".

By default, if no scope is defined in the expression, the scope `$value` is implicitly used.

ScEL supports a number of predefined scopes that can be used for example :

 - To override the output topic.
 - To define record the key to be used.
 - To get access to the source file metadata.
 - Etc.

| Scope | Description | Type |
|--- | --- |--- |
| `{% raw %}{{ $headers }}{% endraw %}` | The record headers  | - |
| `{% raw %}{{ $key }}{% endraw %}` | The record key | `string` |
| `{% raw %}{{ $metadata }}{% endraw %}` | The file metadata  | `struct` |
| `{% raw %}{{ $offset }}{% endraw %}` | The offset information of this record into the source file  | `struct` |
| `{% raw %}{{ system }}{% endraw %}` | The system environment variables and runtime properties | `struct` |
| `{% raw %}{{ $timestamp }}{% endraw %}` | The record timestamp  | `long` |
| `{% raw %}{{ $topic }}{% endraw %}` | The output topic | `string` |
| `{% raw %}{{ $value }}{% endraw %}` | The record value| `struct` |
| `{% raw %}{{ $variables }}{% endraw %}` | The contextual filter-chain variables| `map[string, object]` |

Note, that in case of failures more fields are added to the current filter context (see : [Handling Failures](./handling-failures)

### Record Headers

The scope `headers` allows to defined the headers of the output record.

### Record key

The scope `key` allows to defined the key of the output record. Only string key is currently supported.

### Source Metadata

The scope `metadata` allows read access to information about the file being processing.

| Predefined Fields (ScEL) | Description | Type |
|--- | --- |--- |
| `{% raw %}{{ $metadata.name }}{% endraw %}` | The file name  | `string` |
| `{% raw %}{{ $metadata.path }}{% endraw %}` | The file directory path | `string` |
| `{% raw %}{{ $metadata.absolutePath }}{% endraw %}` | The file absolute path | `string` |
| `{% raw %}{{ $metadata.hash }}{% endraw %}` | The file CRC32 hash | `int` |
| `{% raw %}{{ $metadata.lastModified }}{% endraw %}` | The file last modified time.  | `long` |
| `{% raw %}{{ $metadata.size }}{% endraw %}` | The file size  | `long` |
| `{% raw %}{{ $metadata.inode }}{% endraw %}` | The file Unix inode  | `long` |

## Record Offset

The scope `offset` allows read access to information about the original position of the record into the source file.

| Predefined Fields (ScEL) | Description | Type |
|--- | --- |--- |
| `{% raw %}{{ $offset.startPosition }}{% endraw %}` | The start position of the record into the source file  | `long` |
| `{% raw %}{{ $offset.endPosition }}{% endraw %}` | The end position of the record into the source file  | `long` |
| `{% raw %}{{ $offset.timestamp }}{% endraw %}` | The creation time of the record (millisecond)  | `long` |
| `{% raw %}{{ $offset.size }}{% endraw %}` | The size in bytes  | `long` |
| `{% raw %}{{ $offset.row }}{% endraw %}` | The row number of the record into the source | `long` |

## System

The scope `system` allows read access to system environment variables and runtime properties.

| Predefined Fields (ScEL) | Description | Type |
|--- | --- |--- |
| `{% raw %}{{ $system.env }}{% endraw %}` | The system environment variables.  | `map[string, string]` |
| `{% raw %}{{ $system.props }}{% endraw %}` | The system environment properties. | `map[string, string]` |

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

{% include_relative plan.md %}