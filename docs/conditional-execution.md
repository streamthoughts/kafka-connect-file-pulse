# Conditional execution

A conditional property `if` can be configured on each filter to determine if that filter should be applied or skipped.
When a filter is skipped, message flow to the next filter without any modification.

`if` configuration accepts a Simple Connect Expression that must return to `true` or `false`.
If the configured expression does not evaluate to a boolean value the filter chain will failed.

The`if` property supports ([simple expression](accessing-data-and-metadata))

The boolean value returned from the filter condition can be inverted by setting the property `invert` to `true`.

For example, the below filter will only be applied on message having a log message containing "BadCredentialsException"

```
filters.TagSecurityException.type=io.streamthoughts.kafka.connect.filepulse.filter.AppendFilter
filters.TagSecurityException.if={% raw %}{{ contains(data.logmessage, BadCredentialsException) }}{% endraw %}
filters.TagSecurityException.invert=false
filters.TagSecurityException.field=tags
filters.TagSecurityException.values=SecurityAlert
```

These boolean functions are available for use with `if` configuration :

| Function      | Description   | Syntax   |
| --------------| --------------|-----------|
| `contains` | Returns `true` if an array field's value contains the specified value  | `{% raw %}{{ contains(field, value) }}{% endraw %}` |
| `ends_with`  | Returns `true` if an a string field's value end with the specified string suffix | `{% raw %}{{ ends_with(field, suffix) }}{% endraw %}` |
| `equals` | Returns `true` if an a string or number fields's value equals the specified value | `{% raw %}{{ equals(field, value) }}{% endraw %}` |
| `exists`  | Returns `true` if an the specified field exists | `{% raw %}{{ exists(struct, field) }}{% endraw %}` |
| `is_null`  | Returns `true` if a field's value is null | `{% raw %}{{ is_null(field) }}{% endraw %}` |
| `matches` | Returns `true` if a field's value match the specified regex | `{% raw %}{{ matches(field, regex) }}{% endraw %}` |
| `starts_with` | Returns `true` if an a string field's value start with the specified string prefix | `{% raw %}{{ starts_with(field, prefix) }}{% endraw %}` |


**Limitations** :
 * `if` property does not support binary operator and then a single condition can be configured.
 * condition cannot be used to easily create pipeline branching.

{% include_relative plan.md %}
