---
date: 2022-04-13
title: "FileSystem Listing"
linkTitle: "FileSystem Listing"
weight: 30
description: >
  Learn how to configure Connect FilePulse for listing files from local or remote storage system.
---

The `FilePulseSourceConnector` periodically lists object files that may be streamed into Kafka using the [FileSystemListing](https://github.com/streamthoughts/kafka-connect-file-pulse/blob/master/connect-file-pulse-api/src/main/java/io/streamthoughts/kafka/connect/filepulse/fs/FileSystemListing.java)  
configured in the connector's configuration.

## Supported Filesystems

Currently, Kafka Connect FilePulse supports the following implementations: 
