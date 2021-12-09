---
title: "Connect FilePulse 2.0 is Available 🚀"
linkTitle: "Connect FilePulse 2.0 is Available"
date: 2021-06-10
description: ""
author: Florian Hussonnois ([@fhussonnois](https://twitter.com/fhussonnois))
---

**Connect FilePulse 2.0 is finally here! Here is an overview of what is new:**

## Supported Cloud Storage

Previously, Connect FilePulse was designed to provide direct integration between legacy systems and Apache Kafka. 
But, it could be only used to process and integrate data records from the local filesystem on which the connector was deployed.

As more and more organizations move from on-premises to cloud infrastructure, we've seen a growing demand from developers for the connector to support cloud storage.

Connect FilePulse 2.0 brings you the capabilities for reading files across different storage systems. 
Using a single Kafka Connect Source Connector you can now read files from the local filesystem, Amazon S3, Azure Blob Storage and Google Cloud Storage.

In addition, the connector supports a variety of formats equally for all storage systems, e.g., text files, CSV, XML, JSON, Avro, etc.
At the same time, you can still benefit from the powerful [processing-filters](/kafka-connect-file-pulse/docs/developer-guide/filters-chain-definition/) mechanism of Connect FilePulse to process data records as they are read by the connector.

For example, here is the configuration for reading CSV object files from an Amazon S3 bucket. 
 
```properties
name=connect-file-pulse-amazon-s3-csv
connector.class=io.streamthoughts.kafka.connect.filepulse.source.FilePulseSourceConnector
topic=connect-filepulse-csv-data-records
tasks.max=1

fs.listing.class=io.streamthoughts.kafka.connect.filepulse.fs.AmazonS3FileSystemListing
fs.listing.interval.ms=10000
fs.listing.filters=io.streamthoughts.kafka.connect.filepulse.scanner.local.filter.RegexFileListFilter
file.filter.regex.pattern=.*\\.csv$

fs.cleanup.policy.class=io.streamthoughts.kafka.connect.filepulse.clean.LogCleanupPolicy

aws.access.key.id=xxxxxxxxx
aws.secret.access.key=xxxxxxxxx
aws.s3.region=eu-west-3
aws.s3.bucket.name=connect-filepulse

tasks.reader.class=io.streamthoughts.kafka.connect.filepulse.fs.reader.AmazonS3RowFileInputReader

skip.headers=1
offset.attributes.string=uri

filters=ParseLine
filters.ParseLine.type=io.streamthoughts.kafka.connect.filepulse.filter.DelimitedRowFilter
filters.ParseLine.extractColumnName=headers
filters.ParseLine.trimColumn=true
filters.ParseLine.separator=;
tasks.file.status.storage.bootstrap.servers=kafka101:9092
tasks.file.status.storage.topic=connect-file-pulse-status
tasks.file.status.storage.topic.partitions=10
tasks.file.status.storage.topic.replication.factor=1
```

## Auto-create Internal Topic

By default, Connect FilePulse uses the internal topic `connect-file-pulse-status` to track the current state of each file
being scheduled and processed by tasks. This allows you to deploy Connect FilePulse is a distributed Kafka Connect cluster with each worker only processing a subset of files.

In version 2.0, this topic is will be automatically created by the connector if it doesn't already exist. You can configure the number of partitions, as well as, the replication factor of this topic using the 
new properties `tasks.file.status.storage.topic.partitions` and `tasks.file.status.storage.topic.replication.factor`.

## InMemoryFileObjectStateBackingStore

In version 2.0, we provide a new property `tasks.file.status.storage.class` that can be used to specify the class implementing the `FileObjectStateBackingStore` interface
to be used for storing the status of each file. By default, Connect FilePulse uses the kafka-based implementation called `i.s.k.c.f.state.KafkaFileObjectStateBackingStore`. 

But, in some context, it may be not necessary to deploy Connect FilePulse in distributed mode and this implementation can lead to additional costs if, for example, you are using a fully-managed Apache Kafka service.
So now, we also provide the `i.s.k.c.f.state.InMemoryFileObjectStateBackingStore` implementation to only keep file status in-memory. 

## Improved Scalability

Connect FilePulse can be used to integrate a very large number of files in parallel. 
Unfortunately, too many files to process can result in a too-large message to produce in Kafka for configuring tasks (i.e. `connect-config`).
To solve this blocking issue, in version 2.0, we have added the new property `max.scheduled.files` to limit the maximum number of files that can be scheduled at the same time (Default is `1000`).

## Improved Grok Expression

In a previous version, Connect FilePulse has brought the support for Grok expressions to parse data. 
Since this mechanism has been migrated to a new dedicated project [kafka-connect-transform-grok](https://github.com/streamthoughts/kafka-connect-transform-grok) in order to be able to use Grok expressions with Kafka Connect's a SMTs.
Now, Connect FilePulse directly depends on that project to provide the `GrokFilter` with a unified configuration.

## Full Release Notes

Connect File Pulse 2.0 can be downloaded from the [GitHub Releases Page](https://github.com/streamthoughts/kafka-connect-file-pulse/releases/tag/v2.0.0). 

Members of the open-source community who appear in these release notes:

* @at0dd
* @qgeffard

Thank you for your valuable contributions!

### Features

* [13eed7b](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/13eed7b) feat(plugin): add support for auto-creating the internal topic used by ConnectFilePulse (#139)
* [5c88877](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/5c88877) feat(plugin): add InMemoryStateBackingStore for tracking status of file objects (#138)
* [52adca9](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/52adca9) feat(filesystems): add support for Google Cloud Storage (#121)
* [7b49b81](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/7b49b81) feat(plugin): add new property max.scheduled.files (#122) (#123)
* [390ad82](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/390ad82) feat(filesystems): add support for AWS S3 (#111)
* [92e3341](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/92e3341) feat(filesystems): add support for Azure Blob Storage (#112)
* [685618a](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/685618a) refactor(filters): migrate GrokFilter to use classes from grok-transformer (#118)

### Improvements & Bugfixes
* [6ef5162](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/6ef5162) fix(api): fix decimal numbers not being correctly parsed (#142)
* [6c779af](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/6c779af) refactor(filesystems): make cleanup policy storage aware
* [d13c236](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/d13c236) fix(filesystems): make compression codec more robust to encoding
* [7d2ddac](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/7d2ddac) docs(site): fix DateFilter formats config
* [e222414](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/e222414) fix(api): change digest value to string

### SubTasks
* [1658d35](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/1658d35) refactor(api/filesystems): move FileInputIterator implementation to commons-fs
* [06385b3](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/06385b3) refactor(filesystems): add module filepulse-commons-fs
* [57da04c](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/57da04c) subtask(all): refactor FilePulse API to support remote storages (#100)
* [ee4acad](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/ee4acad) add github workflow
* [a3eb908](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/a3eb908) build(all): update to java 11
* [98eb51f](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/98eb51f) build(mvn): add maven-wrapper

### Braking changes

* Configurations for Connect FilePulse 1.x  is not compatible with the version 2.x.

If you enjoyed reading this post, check out Connect FilePulse at GitHub and give us a ⭐!
