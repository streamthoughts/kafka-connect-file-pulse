---
date: 2022-06-08
title: "Tracking File Status"
linkTitle: "Tracking File Status"
weight: 90
description: >
  Learn how Connect FilePulse tracks the processing of each input file.
---

Connect File Pulse uses an internal topic (*default:`connect-file-pulse-status`*) to track the current state of files being processed.
This topic is used internally by Tasks to communicate to the SourceConnector instance, but you can easily use it to monitor files progression.

## The message format
Status event are published into JSON with the following schema :

```json
{
  "hostname": {
    "type": "string",
    "description": "The machine from which the source file is read."
  },
  "status": {
    "type": "string",
    "description": "The current status"
  },
  "metadata": {
    "name": {
      "type": "string",
      "description": "The file name."
    },
    "path": {
      "type": "string",
      "description": "The file absolute path."
    },
    "size": {
      "type": "int",
      "description": "The file size."
    },
    "lastModified": {
      "type": "long",
      "description": "The file last-modified property."
    },
    "inode": {
      "type": "int",
      "description": "The file inode"
    },
    "hash": {
      "type": "int",
      "description": "CRC32"
    }
  },
  "offset": {
    "position": {
      "type": "long",
      "description": "The current position in the source file (default : -1)."
    },
    "rows": {
      "type": "long",
      "description": "The number of rows already read from the source file (default : -1)."
    },
    "timestamp": {
      "type": "long",
      "description": "The offset timestamp"
    }
  }
}
```

## List of File Status

An object file can be in the following states :

* \[1\] **SCHEDULED** : The file has been scheduled by the connector monitoring thread.
* \[2\] **INVALID** :  The file can't be scheduled because it is not readable.
* \[2\] **STARTED** : The file is starting to be read by a task.
* \[3\] **READING** : The task is still processing the file. An event is wrote into Kafka while committing offsets.
* \[4\] **FAILED** : The file processing failed.
* \[4\] **COMPLETED** : The task completes the processing of the file.
* \[4\] **COMMITTED** : The task committed the offsets of the completed file.
* \[5\] **CLEANED** :  The file has been successfully clean up by the connector (depends on the configured policy).