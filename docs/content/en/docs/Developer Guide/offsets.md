---
date: 2022-03-02
title: "Identifying Files"
linkTitle: "Identifying Files"
weight: 45
description: >
  Learn how Kafka Connect FilePulse uniquely identifies files.
---

Kafka Connect FilePulse uses a pluggable interface called [`SourceOffsetPolicy`](https://github.com/streamthoughts/kafka-connect-file-pulse/blob/master/connect-file-pulse-api/src/main/java/io/streamthoughts/kafka/connect/filepulse/source/SourceOffsetPolicy.java) for 
uniquely identifying files. Basically, the implementation passed in the connector's configuration is used for computing a unique identifier which is
used by Kafka Connect to persist the position of the connector for each file (i.e., the offsets saved in the `connect-offsets` topic).

By default, Kafka Connect FilePulse use the default implementation `DefaultSourceOffsetPolicy` which accepts the following configuration: 

| Configuration              | Description                                                                                                                                                                                                                         | Type     | Default | Importance |
|----------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|---------|------------|
| `offset.attributes.string` | A separated list of attributes, using '+' character as separator, to be used for uniquely identifying an object file; must be one of [name, path, lastModified, inode, hash, uri] (e.g: name+hash). Note that order doesn't matter. | `string` | `uri`   | HIGH       |
