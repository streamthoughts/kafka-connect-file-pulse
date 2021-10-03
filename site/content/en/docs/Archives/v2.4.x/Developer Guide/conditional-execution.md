---
date: 2020-09-30
title: "Conditional Execution"
linkTitle: "Conditional Execution"
weight: 60
description: >
  Learn how to conditionally execute a transformation filter.
---

A conditional property `if` can be configured on each filter to determine if that filter should be applied or skipped.
When a filter is skipped, message flow to the next filter without any modification.

`if` configuration accepts a Simple Connect Expression that must return to `true` or `false`.
If the configured expression does not evaluate to a boolean value the filter chain will fail.

The`if` property supports ([simple expression](accessing-data-and-metadata))

The boolean value returned from the filter condition can be inverted by setting the property `invert` to `true`.

For example, the below filter will only be applied on message having a log message containing "BadCredentialsException"

```
filters.TagSecurityException.type=io.streamthoughts.kafka.connect.filepulse.filter.AppendFilter
filters.TagSecurityException.if={{ contains(data.logmessage, BadCredentialsException) }}
filters.TagSecurityException.invert=false
filters.TagSecurityException.field=tags
filters.TagSecurityException.values=SecurityAlert
```

These conditional functions are available for use with the `if` config property :

| Function      | Since   | Description   | Syntax   |
| --------------| --------| -------|-----------|
| `and`         | `2.4.0` | Checks if all of the given conditional expressions are `true`.  | `{{ and(booleanExpression1, booleanExpression2, ...) }}` |
| `contains`    |         | Returns `true` if an array field's value contains the specified value  | `{{ contains(field, value) }}` |
| `ends_with`   |         | Returns `true` if an a string field's value end with the specified string suffix | `{{ ends_with(field, suffix) }}` |
| `equals`      |         | Returns `true` if an a string or number fields's value equals the specified value | `{{ equals(field, value) }}` |
| `exists`      |         | Returns `true` if an the specified field exists | `{{ exists(struct, field) }}` |
| `gt`          | `2.4.0` | Executes "*greater than operation*" on two values and returns `true` if the first value is greater than the second value, `false`, otherwise. | `{{ gt(expressionValue1, expressionValue2) }}` |
| `is_null`     |         | Returns `true` if a field's value is null | `{{ is_null(field) }}` |
| `lt`          | `2.4.0` | Executes "*less than operation*" on two values and returns `true` if the first value is less than the second value, `false`, otherwise. | `{{ lt(expressionValue1, expressionValue2) }}` |
| `matches`     |         | Returns `true` if a field's value match the specified regex | `{{ matches(field, regex) }}` |
| `starts_with` |         | Returns `true` if an a string field's value start with the specified string prefix | `{{ starts_with(field, prefix) }}` |
| `or`          | `2.4.0` | Checks if at least one of the given conditional expressions is `true`..  | `{{ or(booleanExpression1, booleanExpression2, ...) }}` |

**Limitations** :
 * conditions cannot be used to easily create pipeline branching.
