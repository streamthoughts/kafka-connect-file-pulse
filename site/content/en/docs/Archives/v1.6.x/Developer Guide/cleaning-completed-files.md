---
date: 2020-09-10
title: "File Cleanup Policies"
linkTitle: "File Cleanup Policies"
weight: 100
description: >
  The commons configuration for Connect File Pulse.
---

The connector can be configured with a specific [FileCleanupPolicy](connect-file-pulse-api/src/main/java/io/streamthoughts/kafka/connect/filepulse/clean/FileCleanupPolicy.java) implementation.

The cleanup policy can be configured with the below connect property :

| Configuration |   Description |   Type    |   Default |   Importance  |
| --------------| --------------|-----------| --------- | ------------- |
|`fs.cleanup.policy.class` | The fully qualified name of the class which is used to cleanup files | class | *-* | high |


## Available Cleanup Policies

### DeleteCleanPolicy

This policy deletes all files regardless of their final status (completed or failed).

To enable this policy, the property `fs.cleanup.policy.class` must configured to : 

```
io.streamthoughts.kafka.connect.filepulse.clean.DeleteCleanupPolicy
```

#### Configuration
no configuration

### LogCleanPolicy

This policy prints to logs some information after files completion.

To enable this policy, the property `fs.cleanup.policy.class` must configured to : 

```
io.streamthoughts.kafka.connect.filepulse.clean.LogCleanupPolicy 
```

#### Configuration
no configuration

### MoveCleanPolicy

This policy attempts to move atomically files to configurable target directories.

To enable this policy, the property `fs.cleanup.policy.class` must configured to : 

```
io.streamthoughts.kafka.connect.filepulse.clean.MoveCleanupPolicy
```

#### Configuration

| Configuration |   Description |   Type    |   Default |   Importance  |
| --------------| --------------|-----------| --------- | ------------- |
|`cleaner.output.failed.path` | Target directory for file proceed with failure | string | *.failure* | high |
|`cleaner.output.succeed.path` | Target directory for file proceed successfully | string | *.success* | high |

## Implementing your own policy