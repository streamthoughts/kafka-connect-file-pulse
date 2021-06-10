---
date: 2021-05-19
title: "Tracking File Status"
linkTitle: "Tracking File Status"
weight: 90
description: >
  Learn how Connect FilePulse tracks the processing of each input file.
---

Connect File Pulse use an internal topic (*default:`connect-file-pulse-status`*) to track the current state of files being processed.
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

## List of File Status

Source file can be in the following states :

* \[1\] **SCHEDULED** :  The file has been scheduled by the connector monitoring thread.
* \[2\] **INVALID** :  The file can't be scheduled because it is not readable.
* \[2\] **STARTED** : The file is starting to be read by a Task.
* \[3\] **READING** : The file is currently being read by a task. An event is wrote into Kafka while committing offsets.
* \[4\] **FAILED** : The file processing failed.
* \[4\] **COMPLETED** : The file processing is completed.
* \[5\] **CLEANED** :  The file has been successfully clean up (depending of the configured strategy).

## Configure Kafka reporter

| Configuration |   Description |   Type    |   Default |   Importance  |
| --------------| --------------|-----------| --------- | ------------- |
|`tasks.file.status.storage.topic` | Name of the internal topic used by tasks and connector to report and monitor file progression. | class | *connect-file-pulse-status* | high |
|`tasks.file.status.storage.bootstrap.servers` | Reporter identifier to be used by tasks and connector to report and monitor file progression (must be unique per connector). | string | *-* | high |

## Override Internal Consumer/Producer Configuration

To override the default configuration for the internal consumer and producer clients used for reporting states, 
you can used one of the following override prefixes :

* `tasks.file.status.storage.consumer.<consumer_property>`
* `tasks.file.status.storage.producer.<producer_property>`