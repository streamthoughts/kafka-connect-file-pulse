---
date: 2022-03-02
title: "Conditional Execution"
linkTitle: "Conditional Execution"
weight: 60
description: >
  Learn how to conditionally execute a transformation filter.
---

A conditional property `if` can be configured on each filter to determine if that filter should be applied or skipped.
When a filter is skipped, record flow to the next filter without any modification.

`if` configuration accepts a [Simple Connect Expression](../accessing-data-and-metadata) that must return to `TRUE` or `FALSE`.
If the configured expression does not evaluate to a boolean value the filter chain will fail.

The `BOOLEAN` value returned from the filter condition can be inverted by setting the property `invert` to `TRUE`.

For example, the below filter will only be applied on records having a log message containing "BadCredentialsException"

```
filters.TagSecurityException.type=io.streamthoughts.kafka.connect.filepulse.filter.AppendFilter
filters.TagSecurityException.if={{ contains(data.logmessage, BadCredentialsException) }}
filters.TagSecurityException.invert=false
filters.TagSecurityException.field=tags
filters.TagSecurityException.values=SecurityAlert
```

The following list shows the supported functions that return a `BOOLEAN` type for use with the `if` config property:

* [`and`](../accessing-data-and-metadata#and)
* [`contains`](../accessing-data-and-metadata#contains)
* [`ends_with`](../accessing-data-and-metadata#ends_with)
* [`equals`](../accessing-data-and-metadata#equals)
* [`exists`](../accessing-data-and-metadata#exists)
* [`gt`](../accessing-data-and-metadata#gt)
* [`is_null`](../accessing-data-and-metadata#is_null)
* [`lt`](../accessing-data-and-metadata#lt)
* [`matches`](../accessing-data-and-metadata#matches)
* [`starts_with`](../accessing-data-and-metadata#starts_with)
* [`or`](../accessing-data-and-metadata#or)

{{% alert title="Limitations" color="warning" %}}
Conditions cannot be used to easily create pipeline branching.
{{% /alert %}}
