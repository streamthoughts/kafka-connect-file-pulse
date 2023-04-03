# Kafka Connect File Pulse

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/streamthoughts/kafka-connect-file-pulse/blob/master/LICENSE)
[![Stars](https://img.shields.io/github/stars/streamthoughts/kafka-connect-file-pulse)](https://img.shields.io/github/stars/streamthoughts/kafka-connect-file-pulse)
[![Forks](https://img.shields.io/github/forks/streamthoughts/kafka-connect-file-pulse)](https://img.shields.io/github/forks/streamthoughts/kafka-connect-file-pulse)
[![DockerPull](https://img.shields.io/docker/pulls/streamthoughts/kafka-connect-file-pulse)](https://img.shields.io/docker/pulls/streamthoughts/kafka-connect-file-pulse)
[![Issues](https://img.shields.io/github/issues/streamthoughts/kafka-connect-file-pulse)](https://img.shields.io/github/issues/streamthoughts/kafka-connect-file-pulse)
![Main Build](https://github.com/streamthoughts/kafka-connect-file-pulse/actions/workflows/main.yml/badge.svg)

[![Reliability Rating](https://sonarcloud.io/api/project_badges/measure?project=streamthoughts_kafka-connect-file-pulse&metric=reliability_rating)](https://sonarcloud.io/summary/new_code?id=streamthoughts_kafka-connect-file-pulse)
[![Maintainability Rating](https://sonarcloud.io/api/project_badges/measure?project=streamthoughts_kafka-connect-file-pulse&metric=sqale_rating)](https://sonarcloud.io/summary/new_code?id=streamthoughts_kafka-connect-file-pulse)
[![Vulnerabilities](https://sonarcloud.io/api/project_badges/measure?project=streamthoughts_kafka-connect-file-pulse&metric=vulnerabilities)](https://sonarcloud.io/summary/new_code?id=streamthoughts_kafka-connect-file-pulse)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=streamthoughts_kafka-connect-file-pulse&metric=coverage)](https://sonarcloud.io/summary/new_code?id=streamthoughts_kafka-connect-file-pulse)

<p align="center">
<img width="400" height="400" src="https://github.com/streamthoughts/kafka-connect-file-pulse/raw/master/assets/logo.png">
</p>

**Connect FilePulse** is a multipurpose, scalable and reliable,
[Kafka Connector](http://kafka.apache.org/documentation.html#connect) that makes it easy to parse, transform and stream any file, in any format, into Apache Kafka™.
It provides capabilities for reading files from: **local-filesystem**, **Amazon S3**, **Azure Storage** and **Google Cloud Storage**.

## Motivation

In organizations, data is frequently exported, shared and integrated from legacy systems through the use of
files in a wide variety of formats (e.g. CSV, XML, JSON, Avro, etc.). Dealing with all of these formats can
quickly become a real challenge for enterprise that usually end up with a complex and hard
to maintain data integration mess.


A modern approach consists in building a scalable data streaming platform as a central nervous
system to decouple applications from each other. **Apache Kafka™** is one of the most widely
used technologies to build such a system. The Apache Kafka project packs with Kafka Connect a distributed,
fault tolerant and scalable framework for connecting Kafka with external systems.

The **Connect File Pulse** project aims to provide an easy-to-use solution, based on Kafka Connect,
for streaming any type of data file with the Apache Kafka™ platform.

Some of the features of Connect File Pulse are inspired by the ingestion capabilities of **Elasticsearch** and **Logstash**.

## 🚀 Key Features Overview

Connect FilePulse provides a set of built-in features for streaming files from multiple filesystems into Kafka. This includes, among other things:

* Support for recursive scanning of local directories.
* Support for reading files from Amazon S3, Azure Storage and Google Cloud Storage.
* Support multiple input file formats (e.g: CSV, JSON, AVRO, XML).
* Support for Grok expressions.
* Parsing and transforming data using built-in or custom processing filters.
* Error handler definition
* Monitoring files while they are being written into Kafka
* Support pluggable strategies to clean up completed files
* Etc.

## 🙏 Show your support

You think this project can help you or your team to ingest data into Kafka ?
Please 🌟 this repository to support us!

## 🏁 How to get started ?

The best way to learn Kafka Connect File Pulse is to follow the step by step [Getting Started](https://streamthoughts.github.io/kafka-connect-file-pulse/docs/getting-started/).

If you want to read about using Connect File Pulse, the full documentation can be found [here](https://streamthoughts.github.io/kafka-connect-file-pulse/)

**File Pulse** is also available on [Docker Hub](https://hub.docker.com/r/streamthoughts/kafka-connect-file-pulse) 🐳

```bash
https://hub.docker.com/r/streamthoughts/kafka-connect-file-pulse:latest
```

## 💡 Contributions

Any feedback, bug reports and PRs are greatly appreciated! See our [guideline](./CONTRIBUTING.md).

* Source Code: [https://github.com/streamthoughts/kafka-connect-file-pulse](https://github.com/streamthoughts/kafka-connect-file-pulse)
* Issue Tracker: [https://github.com/streamthoughts/kafka-connect-file-pulse/issues](https://github.com/streamthoughts/kafka-connect-file-pulse/issues)

* Documentation: [https://streamthoughts.github.io/kafka-connect-file-pulse/](https://streamthoughts.github.io/kafka-connect-file-pulse/)
* Releases: [https://github.com/streamthoughts/kafka-connect-file-pulse/releases](https://github.com/streamthoughts/kafka-connect-file-pulse/releases)

## Licence

Copyright 2019-2021 StreamThoughts.

Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

[http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0)

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License
