---
date: 2020-05-20
title: "FAQ"
linkTitle: "FAQ"
weight: 97
description: >
  The most frequently asked questions ?
---

## Could we deployed kafka-connect-file-pulse in distributed mode ?

Connect File Pulse must be running locally to the machine hosting files to be ingested. It is recommend to deploy your connector in distributed mode. Multiple Kafka Connect workers can be deployed on the same machine and participating in the same cluster. The configured input directory will be scanned by the JVM running the SourceConnector. Then, all detected files will be scheduled amongs the tasks spread on your local cluster.

## Is kafka-connect-file-pulse fault-tolerant ?

Connect File Pulse guarantees no data loss by leveraging Kafka Connect fault-tolerance capabilities.
Each task keeps a trace of the file offset of the last record written into Kafka. In case of a restart, tasks will continue where they stopped before crash.
Note, that some duplicates maybe written into Kafka.

## Is kafka-connect-file-pulse could be used in place of other solutions like Logstash ?

Connect File Pulse has some features which are similar to the ones provided by Logstash [codecs](https://www.elastic.co/guide/en/logstash/current/codec-plugins.html)/[filters](https://www.elastic.co/guide/en/logstash/current/filter-plugins.html). Filters like GrokFilter are actually strongly inspired from Logstash. For example you can use it to parse non-structured data like application logs.

However, Connect File Pulse has not be originally designed to collect dynamic application log files.