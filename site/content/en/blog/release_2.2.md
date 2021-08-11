---
title: "Connect FilePulse 2.2 is Released 🚀"
linkTitle: "Connect FilePulse 2.2 is Released"
date: 2021-08-10
description: ""
author: Florian Hussonnois ([@fhussonnois](https://twitter.com/fhussonnois))
---

**This new release brings new capabilities and several bug fixes and improvements that make ConnectFilePulse still the more powerful Kafka Connect-based solution for processing files.**

## Support for file-listing process delegation

From the very beginning, Connect FilePulse was designed differently from most other Kafka connectors used for file processing. 
For example, we chose to use a single thread called the `FileSystemMonitorThread, managed by the connector instance, to scan the files to be processed and to apply the configured file cleanup policy.
Then, when new files are detected on the file system, the connector instance triggers a task reconfiguration to distribute the files to be processed among the tasks.

Although this design offers many advantages it also brings some limitations that may make the connector less suitable for some scenarios, 
such as processing a very large number of small files that would be created quickly on the file system.

This limitation is mainly due to the fact that every time a task reconfiguration is triggered, 
Kafka Connect needs to stop and restart all tasks of our connector using the internal Kafka rebalance protocol.
Thus, the connector may have some scalability issues if it is necessary to reconfigure tasks every second because new files have been created on the local filesystem.

To support such a scenario, Connect FilePulse 2.2.0 brings a new feature to delegate the file listing process to the connector's tasks
This new feature can be enabled by setting the connector's property `fs.listing.task.delegation.enabled` to `true`.

When enabled, each task will scan the filesystem using the `fs.listing.class` passed through the connector's configuration.
In addition, a dedicated `TaskPartitioner` is used to partition each file to a single task using the murmur2 hash algorithm.
Finally, the cleanup policy passed through the connector's configuration is still executed by the `FileSystemMonitorThread`.

## TaskPartitioner

Additionally, Connect FilePulse 2.2.0 introduces a new pluggable interface called `TaskPartitioner` used to partition files among the connector's tasks.
The connector ships with two built-in implementations: the `DefaultTaskPartitioner` that spreads files evenly among the tasks and the `HashByURITaskPartitioner` that partitions each file based on its URI.

## Improved support for XML 

Connect FilePulse 2.2.0 adds various improvements for XML support. 
So now, when using Connect FilePulse with the `LocalXMLFileInputReader` you can enable the following features: 

* `reader.xml.parser.validating.enabled=true`: To specify that the XML parser should validate documents as they are parsed.
* `reader.xml.parser.namespace.aware.enabled=true`: To specify that the XML parser should provide support for XML namespaces.
* `reader.xml.exclude.empty.elements`: To specify that the reader should automatically exclude elements having no field.

Furthermore, dynamic schema resolution has been improved when processing complex XML documents and more especially when handling documents with elements containing arrays.

## Improved LastModifiedFileListFilter

Finally, this new release enhances the `LastModifiedFileListFilter` to allow configuring the maximum age in milliseconds of a file to be eligible for processing. 
For this, you can use the new connector's configuration property: `file.filter.maximum.age.ms`.

## Full Release Notes

Connect File Pulse 2.2 can be downloaded from the [GitHub Releases Page](https://github.com/streamthoughts/kafka-connect-file-pulse/releases/tag/v2.2.0). 

### New Features

[5bc2024](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/5bc2024) feat(plugin): allow excluding from processing files based on maximum age in ms (#161)
[a3ef5db](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/a3ef5db) feat(filesystems): improve XMLFileInputIterator to allow excluding empty element (#159)
[b00ef42](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/b00ef42) feat(scripts): add arg to specify number of connect workers
[ef8fbc3](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/ef8fbc3) feat(plugin): add support for task file listing delegation
[137c1a6](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/137c1a6) feat(filesystems): enhance XMLFileInputIterator with new config props

### Improvements & Bugfixes

[a693b52](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/a693b52) fix(plugin): fix FilePulseSourceConnector should raise an error when FileSystemMonitorThread crash
[c6f1f13](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/c6f1f13) fix(api): fix empty document removing in XMLFileInputIterator
[c0cc249](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/c0cc249) fix(api): improve support for XML by adding capabilities to merge schemas (#160)

### Docs
[0c92901](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/0c92901) docs(site): fix page date and css
[7000b84](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/7000b84) docs(site): add release note for 2.1.0

### Sub-Tasks

[4f0add2](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/4f0add2) project(issue): add github stale bot config
[34ab52a](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/34ab52a) fix(scripts): update docker-compose for debug
[a2fb4a5](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/a2fb4a5) sub-task(plugin): add new interface TaskPartitioner
[bbeebda](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/bbeebda) subtask(plugin): add new interface FileURIProvider
[e2825c0](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/e2825c0) build(deps): bump commons-compress from 1.20 to 1.21

If you enjoyed reading this post, check out Connect FilePulse at GitHub and give us a ⭐!
