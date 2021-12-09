---
date: 2021-06-10
title: "File Cleanup Policies"
linkTitle: "File Cleanup Policies"
weight: 100
description: >
  The commons configuration for Connect File Pulse.
---

The connector can be configured with a specific [FileCleanupPolicy](https://github.com/streamthoughts/kafka-connect-file-pulse/blob/master/connect-file-pulse-api/src/main/java/io/streamthoughts/kafka/connect/filepulse/clean/FileCleanupPolicy.java) implementation.

The cleanup policy can be configured with the below connect property :

| Configuration |   Description |   Type    |   Default |   Importance  |
| --------------| --------------|-----------| --------- | ------------- |
|`fs.cleanup.policy.class` | The fully qualified name of the class which is used to cleanup files | class | *-* | high |


## Available Cleanup Policies

### `DeleteCleanPolicy`

This policy deletes all files regardless of their final status (completed or failed).

To enable this policy, the property `fs.cleanup.policy.class` must configured to : 

```
io.streamthoughts.kafka.connect.filepulse.fs.clean.DeleteCleanupPolicy
```

### `LogCleanPolicy`

This policy prints to logs some information after files completion.

To enable this policy, the property `fs.cleanup.policy.class` must configured to : 

```
io.streamthoughts.kafka.connect.filepulse.fs.clean.LogCleanupPolicy 
```

### `LocalMoveCleanPolicy`

This policy attempts to move atomically files to configurable target directories.

To enable this policy, the property `fs.cleanup.policy.class` must configured to : 

```
io.streamthoughts.kafka.connect.filepulse.fs.clean.LocalMoveCleanPolicy
```

{{% alert title="Usage" color="warning" %}}
This policy only works when using the `LocalFSDirectoryListing`.
{{% /alert %}}

#### Configuration

| Configuration |   Description |   Type    |   Default |   Importance  |
| --------------| --------------|-----------| --------- | ------------- |
|`cleaner.output.failed.path` | Target directory for file proceed with failure | string | *.failure* | high |
|`cleaner.output.succeed.path` | Target directory for file proceed successfully | string | *.success* | high |

## Implementing your own policy