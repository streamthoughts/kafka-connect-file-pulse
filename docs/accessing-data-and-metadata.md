# Accessing Data and Metadata

Some filter properties (like : [AppendFilter](#appendfilter)) can be configured using *Simple Connect Expression Language*.

*Simple Connect Expression Language* (ScEL for short) is an expression language based on regex that allow quick access and manipulating record fields and metadata.

The syntax to define an expression is of the form : "`{% raw %}{{ <expression string> }}{% endraw %}`".

ScEL supports the following functionality :

* Field Selector
* Nested Navigation
* String substitution
* Functions

## Field Selector

Expression language can be used to easily select one field from the input record : "`{% raw %}{{ username }}{% endraw %}`."

## Nested Navigation

To navigate down a struct value, just use a period to indicate a nested field value : "`{% raw %}{{ address.city }}{% endraw %}`."

## String substitution

Expression language can be used to easily build a new string field that concatenate multiple ones : "`{% raw %}{{ <expression one> }}-{{ <expression two>}}{% endraw %}`."

## Functions

ScEL supports a number of predefined functions that can be used to apply a single transformation on a field.

| Function       | Description   | Example   |
| ---------------| --------------|-----------|
| `converts`     | Converts a field'value into the specified type | `{% raw %}{{ converts(my_field, INTEGER) }}{% endraw %}` |
| `extract_array`| Returns the element at the specified position of the specified array | `{% raw %}{{extract_array(my_array, 0) }}{% endraw %}` |
| `length`       | Returns the number of elements into an array of the length of an string field | `{% raw %}{{ length(my_array) }}{% endraw %}` |
| `lowercase`    | Converts all of the characters in a string field's value to lower case | `{% raw %}{{ lowercase(my_field) }}{% endraw %}` |
| `nlv`          | Sets a default value if a field's value is null | `{% raw %}{{ length(my_array) }}{% endraw %}` |
| `uppercase`    | Converts all of the characters in a string field's value to upper case  | `{% raw %}{{ uppercase(my_field) }}{% endraw %}` |

See *Conditional execution* for additional functions.

**Limitations** :

* Current, this is not possible to register user-defined functions (UDFs).

## Pre-defined fields

Connect File Pulse pre-defined some special fields prefixed with `$`. Those fields are not part of final record write into Kafka.

| Predefined Fields / ScEL | Description | Type |
|--- | --- |--- |
| `{% raw %}{{ $file.name }}{% endraw %}` | The file name  | `string` |
| `{% raw %}{{ $file.path }}{% endraw %}` | The file directory path | `string` |
| `{% raw %}{{ $file.absPath }}{% endraw %}` | The file absolute path | `string` |
| `{% raw %}{{ $file.hash }}{% endraw %}` | The file CRC32 hash | `int` |
| `{% raw %}{{ $file.lastModified }}{% endraw %}` | The file last modified time.  | `long` |
| `{% raw %}{{ $file.size }}{% endraw %}` | The file size  | `long` |
| `{% raw %}{{ $offset }}{% endraw %}` | The position of the record into the source file  | `long` |
| `{% raw %}{{ $row }}{% endraw %}` | The row number of the record into the source | `long` |

Note, that in case of failures more fields are added to the current filter context (see : [Handling Failures](./handling-failures)

{% include_relative plan.md %}