---
date: 2022-03-02
title: "File Cleanup Policies"
linkTitle: "File Cleanup Policies"
weight: 100
description: >
  The commons configuration for Connect File Pulse.
---

The connector can be configured with a specific [FileCleanupPolicy](https://github.com/streamthoughts/kafka-connect-file-pulse/blob/master/connect-file-pulse-api/src/main/java/io/streamthoughts/kafka/connect/filepulse/clean/FileCleanupPolicy.java) implementation.

The cleanup policy can be configured with the below connect property :

| Configuration             | Description                                                          | Type  | Default | Importance |
|---------------------------|----------------------------------------------------------------------|-------|---------|------------|
| `fs.cleanup.policy.class` | The fully qualified name of the class which is used to cleanup files | class | *-*     | high       |


## Generic Cleanup Policies

### `DeleteCleanPolicy`

This policy deletes all files regardless of their final status (completed or failed).

To enable this policy, the property `fs.cleanup.policy.class` must be configured to : 

```
io.streamthoughts.kafka.connect.filepulse.fs.clean.DeleteCleanupPolicy
```

### `LogCleanPolicy`

This policy prints into logs some information after files completion.

To enable this policy, the property `fs.cleanup.policy.class` must be configured to : 

```
io.streamthoughts.kafka.connect.filepulse.fs.clean.LogCleanupPolicy 
```

## Cleanup Policies: Local Filesystem

### `LocalMoveCleanupPolicy`

This policy attempts to move atomically files to configurable target directories.

To enable this policy, the property `fs.cleanup.policy.class` must be configured to : 

```
io.streamthoughts.kafka.connect.filepulse.fs.clean.LocalMoveCleanupPolicy
```

{{% alert title="Usage" color="warning" %}}
This policy only works when using the `LocalFSDirectoryListing`.
{{% /alert %}}

#### Configuration

| Configuration                 | Description                                    | Type   | Default    | Importance |
|-------------------------------|------------------------------------------------|--------|------------|------------|
| `cleaner.output.failed.path`  | Target directory for file proceed with failure | string | *.failure* | high       |
| `cleaner.output.succeed.path` | Target directory for file proceed successfully | string | *.success* | high       |

## Cleanup Policies: Amazon

### `AmazonMoveCleanupPolicy`

This policy moves S3 objects atomically files to configurable target directories.

To enable this policy, the property `fs.cleanup.policy.class` must be configured to :

```
io.streamthoughts.kafka.connect.filepulse.fs.clean.AmazonS3MoveCleanupPolicy
```


| Configuration                                    | Description                                                                                                                                                                                                                                                | Type     | Default                               | Importance |
|--------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|---------------------------------------|------------|
| `fs.cleanup.policy.move.success.aws.bucket.name` | The name of the destination S3 bucket for success objects (optional)                                                                                                                                                                                       | `string` | *Bucket name of the source S3 Object* | HIGH       |
| `fs.cleanup.policy.move.success.aws.prefix.path` | The prefix to be used for defining the key of an S3 object to move into the destination bucket.                                                                                                                                                            | `string` |                                       | HIGH       |
| `fs.cleanup.policy.move.failure.aws.bucket.name` | The name of the destination S3 bucket for failure objects (optional)                                                                                                                                                                                       | `string` | *Bucket name of the source S3 Object* | HIGH       |
| `fs.cleanup.policy.move.failure.aws.prefix.path` | The prefix to be used for defining the key of an S3 object to move into the destination bucket.                                                                                                                                                            | `string` |                                       | HIGH       |
| `aws.s3.default.object.storage.class`            | The AWS storage class to associate with an S3 object when it is copied by the connector (e.g., during a move operation). Accepted values are: `STANDARD`, `GLACIER`, `REDUCED_REDUNDANCY`, `STANDARD_IA`,`ONEZONE_IA`,`INTELLIGENT_TIERING`,`DEEP_ARCHIVE` | `string` |                                       | LOW        |

## Implementing your own policy