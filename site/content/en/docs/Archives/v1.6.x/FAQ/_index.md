---
date: 2020-05-25
title: "FAQ"
linkTitle: "FAQ"
weight: 97
description: >
  The most frequently asked questions ?
---

## Could we deployed FilePulse connector in distributed mode ?

Connect File Pulse must be running locally to the machine hosting files to be ingested. It is recommend to deploy your connector in distributed mode. Multiple Kafka Connect workers can be deployed on the same machine and participating in the same cluster. The configured input directory will be scanned by the JVM running the SourceConnector. Then, all detected files will be scheduled amongs the tasks spread on your local cluster.

## Is FilePulse connector fault-tolerant ?

Connect File Pulse guarantees no data loss by leveraging Kafka Connect fault-tolerance capabilities.
Each task keeps a trace of the file offset of the last record written into Kafka. In case of a restart, tasks will continue where they stopped before crash.
Note, that some duplicates maybe written into Kafka.

## Is FilePulse connector could be used in place of other solutions like Logstash ?

Connect File Pulse has some features which are similar to the ones provided by Logstash [codecs](https://www.elastic.co/guide/en/logstash/current/codec-plugins.html)/[filters](https://www.elastic.co/guide/en/logstash/current/filter-plugins.html). Filters like GrokFilter are actually strongly inspired from Logstash. For example you can use it to parse non-structured data like application logs.

However, Connect File Pulse has not be originally designed to collect dynamic application log files.

## Is FilePulse connector support SASL/SSL authentication mechanisms and can be deployed on Confluent Cloud ?

Yes, FilePulse connector can be deployed on any Kafka Cluster. However, the connector currently requires the use of an internal topic
to synchronize the Connector instance and the Tasks that process files. For doing this, the connector
will create both a producer and consumer clients that you must configured when running the connector with a secured Kafka Cluster.

To override the default configuration for the internal consumer and producer clients, 
you can used one of the following override prefixes :

* `internal.kafka.reporter.consumer.<consumer_property>`
* `internal.kafka.reporter.producer.<producer_property>`

## What are the differences between FilePulse connector and others Kafka connectors for streaming files ?

The following table shows a simple comparison between Connect File Pulse and other solutions : [Connect Spooldir](https://github.com/jcustenborder/kafka-connect-spooldir) and Connect [FileStreams](https://github.com/apache/kafka/tree/trunk/connect/file)

|                                                       | Connect FilePulse  | Connect Spooldir | Connect FileStreams |
|:---                                                   |            :---:   |  :---:           |   :---:              |
| **Connector Type**                                    | source             | source           | source / sink        |
| **License**                                           |Apache License 2.0  |Apache License 2.0| Apache License 2.0   |
| **Available on Confluent Hub**                        | YES                | YES              | YES                  |
| **Docker image**                                      | YES                | NO               | NO                   |
| **Delivery semantics**                                | At-least-once      | At-least-once    | At-most-once         |
| **Usable in production**                              | YES                | YES              | NO                   |
| **Supported file formats(out-of-the box)**            | Delimited, Binary, JSON, Avro, XML (limited) | Delimited, JSON | Text file                | YES              | NO                   |
| **Support recursive directory scan**                  | YES                | NO               | NO                   |
| **Support Archive and Compressed files**              | YES (`GZIP`, `TAR`, `ZIP`) | NO | NO |
| **Source partitions**                                 | Configurable (filename, path, filename+hash) | filename | filename |
| **Support for multi-tasks**                           | YES                | YES | NO |
| **Support for worker distributed mode**               | YES (requires a shared volume) | NO | NO |
| **Support for streaming log files**                   | YES | NO | YES |
| **Support for transformation**                        | Single Message Transforms <br /> Processing Filters (Grok, Append, JSON, etc) | Single Message Transforms| * Single Message Transforms
| **Support for tracking processing progress of files** | YES (using an internal topic)               | NO               | NO                   |