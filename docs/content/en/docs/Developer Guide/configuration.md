---
date: 2022-03-02
title: "Configuration"
linkTitle: "Configuration"
weight: 20
description: >
  The common configurations for deploying a File Pulse connector.
---

## Commons configuration

Whatever the kind of files you are processing a connector should always be configured with the below properties.
These configurations are described in detail in subsequent chapters.

**Common Kafka Connect properties**

| Configuration | Description                                                            | Type   | Default | Importance |
|---------------|------------------------------------------------------------------------|--------|---------|------------|
| `topic`       | The default output topic to write                                      | string | *-*     | high       |
| `tasks.max`   | The maximum number of tasks that should be created for this connector. | string | *-*     | high       |

**Properties for listing and cleaning object files ([FileSystemListing](/kafka-connect-file-pulse/docs/developer-guide/file-system-listing/))**

| Configuration                                  | Description                                                                                                                                                          | Type    | Default                                                                   | Importance |
|------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------|---------------------------------------------------------------------------|------------|
| `fs.listing.class`                             | Class which is used to list eligible files from the scanned file system.                                                                                             | class   | *-*                                                                       | MEDIUM     |
| `fs.listing.filters`                           | Filters use to list eligible input files                                                                                                                             | list    | *-*                                                                       | MEDIUM     |
| `fs.listing.interval.ms`                       | Time interval (in milliseconds) at wish to scan input directory                                                                                                      | long    | *10000*                                                                   | HIGH       |
| `fs.listing.task.delegation.enabled`           | Boolean indicating whether the file listing process should be delegated to tasks.                                                                                    | boolean | *false*                                                                   | LOW        |
| `fs.cleanup.policy.class`                      | The fully qualified name of the class which is used to cleanup files                                                                                                 | class   | *-*                                                                       | HIGH       |
| `fs.cleanup.policy.triggered.on`               | Specify the status when a file get cleanup. Valid values are: `COMPLETED`, `COMMITTED`                                                                               | string  | *COMPLETED*                                                               | MEDIUM     |
| `max.scheduled.files`                          | Maximum number of files that can be schedules to tasks.                                                                                                              | long    | *1000*                                                                    | HIGH       |
| `allow.tasks.reconfiguration.after.timeout.ms` | Specify the timeout (in milliseconds) for the connector to allow tasks to be reconfigured when new files are detected, even if some tasks are still being processed. | long    | *-*                                                                       | LOW        |
| `task.partitioner.class`                       | The TaskPartitioner to be used for partitioning files to tasks.                                                                                                      | class   | `io.streamthoughts.kafka.connect.filepulse.source.DefaultTaskPartitioner` | HIGH       |
| `tasks.halt.on.error`                          | Should a task halt when it encounters an error or continue to the next file.                                                                                         | boolean | *false*                                                                   | HIGH       |
| `tasks.file.processing.order.by`               | The strategy to be used for sorting files for processing. Valid values are: `LAST_MODIFIED`, `URI`, `CONTENT_LENGTH`, `CONTENT_LENGTH_DESC`.                         | string  | `LAST_MODIFIED`                                                           | MEDIUM     |
| `tasks.empty.poll.wait.ms`                     | The amount of time in millisecond a tasks should wait if a poll returns an empty list of records.                                                                    | long    | *500*                                                                     | HIGH       |
| `ignore.committed.offsets`                     | Should a task ignore committed offsets while scheduling a file.                                                                                                      | boolean | *false*                                                                   | LOW        |
| `value.connect.schema`                         | The schema for the record-value.                                                                                                                                     | string  | *-*                                                                       | MEDIUM     |

**Properties for transforming object file record([Filters Chain Definition](/kafka-connect-file-pulse/docs/developer-guide/filters-chain-definition/))**

| Configuration | Description                                                        | Type | Default | Importance |
|---------------|--------------------------------------------------------------------|------|---------|------------|
| `filters`     | List of filters aliases to apply on each data (order is important) | list | *-*     | MEDIUM     |

**Properties for reading object file record([FileReaders](/kafka-connect-file-pulse/docs/developer-guide/file-readers/))**

| Configuration        | Description                                                                      | Type  | Default | Importance |
|----------------------|----------------------------------------------------------------------------------|-------|---------|------------|
| `tasks.reader.class` | The fully qualified name of the class which is used by tasks to read input files | class | *-*     | HIGH       |

**Properties for uniquely identifying object files and records ([FileReaders](/kafka-connect-file-pulse/docs/developer-guide/file-readers/))**

| Configuration         | Description                                                                                            | Type    | Default                                                                      | Importance |
|-----------------------|--------------------------------------------------------------------------------------------------------|---------|------------------------------------------------------------------------------|------------|
| `offset.policy.class` | Class which is used to determine the source partition and offset that uniquely identify a input record | `class` | *io.streamthoughts.kafka.connect.filepulse.offset.DefaultSourceOffsetPolicy* | HIGH       |

**Properties for synchronizing Connector and Tasks**

| Configuration                     | Description                                                                                | Type    | Default                                                                            | Importance |
|-----------------------------------|--------------------------------------------------------------------------------------------|---------|------------------------------------------------------------------------------------|------------|
| `tasks.file.status.storage.class` | The FileObjectStateBackingStore class to be used for storing status state of file objects. | `Class` | `io.streamthoughts.kafka.connect.filepulse.state.KafkaFileObjectStateBackingStore` | HIGH       |

**Available implementations are :**
* `io.streamthoughts.kafka.connect.filepulse.state.InMemoryFileObjectStateBackingStore`
* `io.streamthoughts.kafka.connect.filepulse.state.KafkaFileObjectStateBackingStore`

{{% alert title="Limitation" color="warning" %}}
The `InMemoryFileObjectStateBackingStore` implement is not fault-tolerant and should be only when using Kafka Connect in standalone mode or a single worker.
{{% /alert %}}

**Properties for configuring the `KafkaFileObjectStateBackingStore` class**

| Configuration                                        | Description                                                                                                  | Type   | Default                     | Importance |
|------------------------------------------------------|--------------------------------------------------------------------------------------------------------------|--------|-----------------------------|------------|
| `tasks.file.status.storage.topic`                    | Name of the internal topic used by tasks and connector to report and monitor file progression.               | class  | *connect-file-pulse-status* | HIGH       |
| `tasks.file.status.storage.bootstrap.servers`        | A list of host/port pairs uses by the reporter for establishing the initial connection to the Kafka cluster. | string | *-*                         | HIGH       |
| `tasks.file.status.storage.topic.partitions`         | The number of partitions to be used for the status storage topic.                                            | int    | *-*                         | LOW        |
| `tasks.file.status.storage.topic.replication.factor` | The replication factor to be used for the status storage topic.                                              | float  | *-*                         | LOW        |


**Properties for configuring the `InMemoryFileObjectStateBackingStore` class**

| Configuration                                       | Description                                                 | Type  | Default | Importance | Since  |
|-----------------------------------------------------|-------------------------------------------------------------|-------|---------|------------|--------|
| `tasks.file.status.storage.cache.max.size.capacity` | Specifies the max size capacity of the LRU in-memory cache. | `int` | *10000* | LOW        | v2.5.0 |

In addition, to override the default configuration for the internal consumer and producer clients, 
you can use one of the following override prefixes :

* `tasks.file.status.storage.consumer.<consumer_property>`
* `tasks.file.status.storage.producer.<producer_property>`

## Examples

Some configuration examples are available [here](https://github.com/streamthoughts/kafka-connect-file-pulse/tree/master/examples).

## Defining Connect Record Schema

The optional `value.connect.schema` config property can be used to set the connect-record schema that should be used.
If there is no schema pass through the connector configuration, a schema will be resolved for each record produced.

The `value.connect.schema` must be passed as a JSON string that respects the following schema (using Avro representation):

```json
{
   "type":"record",
   "name":"Schema",
   "fields":[
      {
         "name":"name",
         "type":"string",
         "doc": "The name of this schema"
      },
      {
         "name":"type",
         "type":{
            "type":"enum",
            "name":"Type",
            "symbols":[
               "STRUCT",
               "STRING",
               "BOOLEAN",
               "INT8",
               "INT16",
               "INT32",
               "INT64",
               "FLOAT32",
               "FLOAT64",
               "BYTES",
               "MAP",
               "ARRAY"
            ]
         },
         "doc": "The type of this schema"
      },
      {
         "name":"doc",
         "type":[
            "null",
            "string"
         ],
         "default":null,  
         "doc": "The documentation for this schema"
      },
      {
         "name":"fieldSchemas",
         "type":[
            "null",
            {
               "type":"map",
               "values":"Schema"
            }
         ],
         "default":null,
         "doc": "The fields for this Schema. Throws a DataException if this schema is not a struct."
      },
      {
         "name":"valueSchema",
         "type":[
            "null",
            {
               "type":"map",
               "values":"Schema"
            }
         ],
         "default":null,
         "doc": "The value schema for this map or array schema. Throws a DataException if this schema is not a map or array."
      },
      {
         "name":"keySchema",
         "type":[
            "null",
            {
               "type":"map",
               "values":"Schema"
            }
         ],
         "default":null,
         "doc": "The key schema for this map schema. Throws a DataException if this schema is not a map."
      },
      {
         "name":"defaultValue",
         "type":[
            "null",
            "string"
         ],
         "default":null
      },
      {
         "name":"isOptional",
         "type":"boolean",
         "default":false,
         "doc": "true if this field is optional, false otherwise"
      },
      {
         "name":"version",
         "type":[
            "null",
            "integer"
         ],
         "default":null,
         "doc": "The optional version of the schema. If a version is included, newer versions *must* be larger than older ones."
      }
   ]
}
``` 

**Example:**

```json
{
   "name":"com.example.User",
   "type":"STRUCT",
   "isOptional":false,
   "fieldSchemas":{
      "id":{
         "type":"INT64",
         "isOptional":false
      },
      "first_name":{
         "type":"STRING",
         "isOptional":true
      },
      "last_name":{
         "type":"STRING",
         "isOptional":true
      },
      "email":{
         "type":"STRING",
         "isOptional":true
      },
      "gender":{
         "type":"STRING",
         "isOptional":true
      },
      "country":{
         "type":"STRING",
         "isOptional":true
      },
      "favorite_colors":{
         "type":"ARRAY",
         "isOptional":true,
         "valueSchema": {"type": "STRING"}
      }
   }
}
```
