---
date: 2020-05-20
title: "What is Connect FilePulse ?"
linkTitle: "Connect FilePulse"
weight: 10
description: >
  An introduction to Connect File Pulse
---

## What is it?

**Connect FilePulse** is a polyvalent, scalable and reliable, Apache Kafka Connect plugin that makes it easy to parse, transform and stream any file, in any format, into Apache Kafka™.

{{% alert title="About Connect" color="info" %}}
**[Kafka Connect](https://kafka.apache.org/documentation/#connect)** is a tool for scalably and reliably streaming data between Apache Kafka and other systems. (source: [Apache documentation](https://kafka.apache.org/documentation/#connect)).
{{% /alert %}}

### Key Features

Connect FilePulse provides a set of built-in features for streaming local files into Kafka. This includes, among other things:

* Support for recursive scanning of local directories.
* Reading and writing files into Kafka line by line.
* Support multiple input file formats (e.g: CSV, Avro, XML).
* Parsing and transforming data using built-in or custom processing filters.
* Error handler definition
* Monitoring files while they are being written into Kafka
* Support pluggable strategies to clean up completed files
* Etc.


## Why do I want it?

Connect FilePulse helps you stream local files into Apache Kafka.

* **What is it good for?**: Connect FilePulse lets you define complex pipelines to transform and structure your data before integration into Kafka.

* **What is it not good for?**: Connect FilePulse is maybe not the best solution for collecting application log files.

## Where should I go next?

Give your users next steps from the Overview. For example:

* **Getting Started**: [Get started with Connect FilePulse](/kafka-connect-file-pulse/docs/getting-started/)
* **Examples**: [Check out some example code!](/kafka-connect-file-pulse/docs/examples/)





