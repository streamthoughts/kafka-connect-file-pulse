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

However, Connect File Pulse has not to be originally designed to collect dynamic application log files.

## Is FilePulse connector support SASL/SSL authentication mechanisms and can be deployed on Confluent Cloud ?

Yes, FilePulse connector can be deployed on any Kafka Cluster. However, the connector currently requires the use of an internal topic
to perform synchronization between the _SourceConnector_ instance and the _SourceTasks_ that process files. To do that, FilePulse
will create an internal producer and consumer that you need to configure when running against a secured Kafka Cluster.

To override the default configuration for the internal consumer and producer clients, 
you can use one of the following override prefixes :

* `tasks.file.status.storage.consumer.<consumer_property>`
* `tasks.file.status.storage.producer.<producer_property>`

Example:
```json
{
"tasks.file.status.storage.bootstrap.servers"                             : "CCLOUD_BROKER_SERVICE_URI:9092",
"tasks.file.status.storage.topic"                                         : "connect-file-pulse-status",
"tasks.file.status.storage.producer.security.protocol"                    : "SASL_SSL",
"tasks.file.status.storage.producer.ssl.endpoint.identification.algorithm": "https",
"tasks.file.status.storage.producer.sasl.mechanism"                       : "PLAIN",
"tasks.file.status.storage.producer.sasl.jaas.config"                     : "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"CCLOUD_API_KEY\" password=\"CCLOUD_API_SECRET\";",
"tasks.file.status.storage.producer.request.timeout.ms"                   : "20000",
"tasks.file.status.storage.consumer.security.protocol"                    : "SASL_SSL",
"tasks.file.status.storage.consumer.ssl.endpoint.identification.algorithm": "https",
"tasks.file.status.storage.consumer.sasl.mechanism"                       : "PLAIN",
"tasks.file.status.storage.consumer.sasl.jaas.config"                     : "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"CCLOUD_API_KEY\" password=\"CCLOUD_API_SECRET\";",
"tasks.file.status.storage.consumer.request.timeout.ms"                   : "20000"
}
```

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