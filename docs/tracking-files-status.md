# Tracking file status

Connect File Pulse use an internal topic (*default:connect-file-pulse-status*) to track the current state of files being processed.
This topic is used internally by Tasks to communicate to the SourceConnector instance but you can easily use it to monitor files progression.

## The message format
Status event are publish into JSON with the following schema :

```
{
"hostname": {
    "type": "string",
    "description: "The machine from which the source file is read."
},
"status":{
    "type": "string",
    "description: "The current status"
},
"metadata":{
    "name": {
        "type": "string",
        "description: "The file name."
    },
    "path": {
        "type": "string",
        "description: "The file absolute path."
    },
    "size": {
        "type": "int",
        "description: "The file size."
    },
    lastModified": {
        "type": "long",
        "description: "The file last-modified property."
    },
    "inode": {
        "type": "int",
        "description: "The file inode"
    },
    "hash": {
        "type": "int",
        "description: "CRC32"
    }
},
"offset":{
    "position": {
        "type": "long",
        "description: "The current position in the source file (default : -1)."
    },
    "rows": {
        "type": "long",
        "description: "The number of rows already read from the source file (default : -1)."
    },
    "timestamp": {
        "type": "long",
        "description: "The offset timestamp"
    }
}
```

## Status

Source file can be in the following states :

* \[1\] **SCHEDULED** :  The file has been scheduled by the connector monitoring thread.
* \[2\] **INVALID** :  The file can't be scheduled because it is not readable.
* \[2\] **STARTED** : The file is starting to be read by a Task.
* \[3\] **READING** : The file is currently being read by a task. An event is wrote into Kafka while committing offsets.
* \[4\] **FAILED** : The file processing failed.
* \[4\] **COMPLETED** : The file processing is completed.
* \[5\] **CLEANED** :  The file has been successfully clean up (depending of the configured strategy).

## Configuration

| Configuration |   Description |   Type    |   Default |   Importance  |
| --------------| --------------|-----------| --------- | ------------- |
|`internal.kafka.reporter.topic` | Name of the internal topic used by tasks and connector to report and monitor file progression. | class | *connect-file-pulse-status* | high |
|`internal.kafka.reporter.id` | Group id the internal topic used by tasks and connector to report and monitor file progression | string | *-* | high |
|`internal.kafka.reporter.cluster.bootstrap.servers` | Reporter identifier to be used by tasks and connector to report and monitor file progression (must be unique per connector). | string | *-* | high |

{% include_relative plan.md %}
