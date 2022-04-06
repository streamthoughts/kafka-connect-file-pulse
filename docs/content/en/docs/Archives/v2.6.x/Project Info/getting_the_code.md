---
date: 2020-05-09
title: "Getting the code"
linkTitle: "Getting the code"
weight: 10
description: >
  How to get and build the code ?
---

## Prerequisites for building Connect File Pulse

* Git
* Maven (we recommend version 3.5.3)
* Java 11

## Building Connect File Pulse

The code of Connect File Pulse is kept in GitHub. You can check it out like this:

```bash
$ git clone https://github.com/streamthoughts/kafka-connect-file-pulse.git
```


The project uses Maven, you can build it like this:

```bash
$ cd kafka-connect-file-pulse
$ mvn clean package -DskipTests
```

{{% alert title="Confluent Hub" color="info" %}}
Connect File Pulse is packaged into an archive 
file compatible with [confluent-hub client](https://docs.confluent.io/current/connect/managing/confluent-hub/client.html).
{{% /alert %}}