# Handling Failures

The connector provides some mechanisms to handle failures while executing filters.

By default, the filters chain will immediately failed after an exception is thrown.
But, you can also configure each filter to either ignore errors or to branch to a sub filters-chain.

## Configuration

| Configuration |   Description |   Type    |   Default |   Importance  |
| --------------| --------------|-----------| --------- | ------------- |
| `withOnFailure` | List of filters aliases to apply on each data after failure (order is important). | list | *-* | medium |
| `ignoreFailure` | Ignore failure and continue pipeline filters    | boolean | *false* | medium |


## Ignoring failure

By setting the property `ignoreFailure` to `true`, the filter will be ignored if an exception is thrown.

In that case, the exception is written to the output logs and current data record is simply forwarded to the next filter in the chain.

Using `ignoreFailure=true` can be recommended for optional filters.

### Example

In the below example, the he filter with alias Log4jGrokFilter will be skip in case of failure.

```
filters=Log4jGrokFilter

filters.Log4jGrokFilter.type=io.streamthoughts.kafka.connect.filepulse.filter.GrokFilter
filters.Log4jGrokFilter.match="%{TIMESTAMP_ISO8601:logdate} %{LOGLEVEL:loglevel} %{GREEDYDATA:message}"
filters.Log4jGrokFilter.source=message
filters.Log4jGrokFilter.ignoreFailure=true
```

## Defining error filter chain

A more sophisticated way to handle failures is to define a sub filters-chain on each concern filters.

Sub-filter chains can be defined using the property `withOnFailure`.

### Accessing exception data

Within an error filter chain, some additional fields are available to each filter context.

| Predefined Fields / ScEL | Description | Type |
|--- | --- |--- |
| `{% raw %}{{ $error.message }}{% endraw %}` | The error message  | `string` |
| `{% raw %}{{ $error.filter }}{% endraw %}` | The failed filter name  | `string` |

### Example

In the below example, an `errorMessage` field is added to the record value if the filter with alias Log4jGrokFilter fails.

```
filters=Log4jGrokFilter

filters.Log4jGrokFilter.type=io.streamthoughts.kafka.connect.filepulse.filter.GrokFilter
filters.Log4jGrokFilter.match="%{TIMESTAMP_ISO8601:logdate} %{LOGLEVEL:loglevel} %{GREEDYDATA:message}"
filters.Log4jGrokFilter.source=message
filters.Log4jGrokFilter.overwrite=message
filters.Log4jGrokFilter.withOnFailure=AppendError

filters.AppendError.type=io.streamthoughts.kafka.connect.filepulse.filter.AppendFilter
filters.AppendError.field=errorMessage
filters.AppendError.value="{% raw %}{{ $error.message }}{% endraw %}"

```

{% include_relative plan.md %}