---
date: 2020-05-22
title: "Getting Started"
linkTitle: "Getting Started"
weight: 10
description: >
  Get started with Connect File Pulse through a step by step tutorial.
---

In this tutorial we will explore how to deploy a basic Connect File Pulse connector step by step.

The prerequisites for this tutorial are :

* IDE or Text editor.
* Maven 3+
* Docker (for running a Kafka Cluster 2.x).

## Start Docker Environment

Set the following environment variable to execute next commands.

```bash
$ export GITHUB_REPO_MASTER=https://raw.githubusercontent.com/streamthoughts/kafka-connect-file-pulse/master/
```

**1 ) Run Confluent Platforms with Connect File Pulse**

```bash

$ curl -sSL $GITHUB_REPO_MASTER/docker-compose.yml -o docker-compose.yml
$ docker-compose up -d
```

**2 ) Verify that Connect Worker is running (optional)**
```
$ docker-compose logs "connect-file-pulse"
```

**3 ) Check that Connect File Pulse plugin is correctly loaded (optional)**
```bash
$ curl -sX GET http://localhost:8083/connector-plugins | grep FilePulseSourceConnector
```

## Example : Logs Parsing (Log4j)

This example starts a new connector instance to parse the Kafka Connect container log4j logs before writing them into a configured topic.

**1 ) Start a new connector instance**

```bash
$ curl -sSL $GITHUB_REPO_MASTER/config/connect-file-pulse-quickstart-log4j.json -o connect-file-pulse-quickstart-log4j.json
 
$ curl -sX POST http://localhost:8083/connectors \
-d @connect-file-pulse-quickstart-log4j.json \
--header "Content-Type: application/json" | jq
```

**2 ) Check connector status**
```bash
$ curl -X GET http://localhost:8083/connectors/connect-file-pulse-quickstart-log4j | jq
```

**3 ) Consume output topics**
```bash
$ docker exec -it -e KAFKA_OPTS="" connect kafka-avro-console-consumer \
--topic connect-file-pulse-quickstart-log4j \
--from-beginning \
--bootstrap-server broker:29092 \
--property schema.registry.url=http://schema-registry:8081
```

(output)
```json
...
{"loglevel":{"string":"INFO"},"logdate":{"string":"2019-06-16 20:41:15,247"},"message":{"string":"[main] Scanning for plugin classes. This might take a moment ... (org.apache.kafka.connect.cli.ConnectDistributed)"}}
{"loglevel":{"string":"INFO"},"logdate":{"string":"2019-06-16 20:41:15,270"},"message":{"string":"[main] Loading plugin from: /usr/share/java/schema-registry (org.apache.kafka.connect.runtime.isolation.DelegatingClassLoader)"}}
{"loglevel":{"string":"INFO"},"logdate":{"string":"2019-06-16 20:41:16,115"},"message":{"string":"[main] Registered loader: PluginClassLoader{pluginLocation=file:/usr/share/java/schema-registry/} (org.apache.kafka.connect.runtime.isolation.DelegatingClassLoader)"}}
{"loglevel":{"string":"INFO"},"logdate":{"string":"2019-06-16 20:41:16,115"},"message":{"string":"[main] Added plugin 'org.apache.kafka.common.config.provider.FileConfigProvider' (org.apache.kafka.connect.runtime.isolation.DelegatingClassLoader)"}}
...
```

**4) Observe Connect status**

Connect File Pulse use an internal topic to track the current state of files being processing.

```bash
$ docker exec -it -e KAFKA_OPTS="" connect kafka-console-consumer \
--topic connect-file-pulse-status \
--from-beginning \
--bootstrap-server broker:29092
```

(output)
```json
{"hostname":"f51d45f96ed5","status":"SCHEDULED","metadata":{"name":"kafka-connect.log","path":"/var/log/kafka","size":172559,"lastModified":1560772525000,"inode":1705406,"hash":661976312},"offset":{"position":-1,"rows":0,"timestamp":1560772525527}}
{"hostname":"f51d45f96ed5","status":"STARTED","metadata":{"name":"kafka-connect.log","path":"/var/log/kafka","size":172559,"lastModified":1560772525000,"inode":1705406,"hash":661976312},"offset":{"position":-1,"rows":0,"timestamp":1560772525719}}
{"hostname":"f51d45f96ed5","status":"READING","metadata":{"name":"kafka-connect.log","path":"/var/log/kafka","size":172559,"lastModified":1560772525000,"inode":1705406,"hash":661976312},"offset":{"position":174780,"rows":1911,"timestamp":1560772535322}}
...
```

**5 ) Stop all containers**
```bash
docker-compose down
```

### Example : CSV File Parsing

This example starts a new connector instance that parse a CSV file and filter rows based on column's values before writing record into Kafka.

**1 ) Start a new connector instance**

```bash
$ curl -sSL $GITHUB_REPO_MASTER/config/connect-file-pulse-quickstart-csv.json -o connect-file-pulse-quickstart-csv.json

$ curl -sX POST http://localhost:8083/connectors \
-d @connect-file-pulse-quickstart-csv.json \
--header "Content-Type: application/json" | jq
```

**2 ) Copy example csv file into container**

```bash
$ curl -sSL $GITHUB_REPO_MASTER/examples/quickstart-musics-dataset.csv -o quickstart-musics-dataset.csv
$ docker exec -it connect mkdir -p /tmp/kafka-connect/examples
$ docker cp quickstart-musics-dataset.csv connect://tmp/kafka-connect/examples/quickstart-musics-dataset.csv
```

**3 ) Check connector status**
```bash
$ curl -X GET http://localhost:8083/connectors/connect-file-pulse-quickstart-csv | jq
```

**4 ) Check for task completion**
```
$ docker logs --tail="all" -f connect | grep "Orphan task detected"
```

**5 ) Consume output topics**
```bash
$ docker exec -it connect kafka-avro-console-consumer \
--topic connect-file-pulse-quickstart-csv \
--from-beginning \
--bootstrap-server broker:29092 \
--property schema.registry.url=http://schema-registry:8081
```

(output)
```json
{"title":{"string":"40"},"album":{"string":"War"},"duration":{"string":"02:38"},"release":{"string":"1983"},"artist":{"string":"U2"},"type":{"string":"Rock"}}
{"title":{"string":"Acrobat"},"album":{"string":"Achtung Baby"},"duration":{"string":"04:30"},"release":{"string":"1991"},"artist":{"string":"U2"},"type":{"string":"Rock"}}
{"title":{"string":"Bullet the Blue Sky"},"album":{"string":"The Joshua Tree"},"duration":{"string":"04:31"},"release":{"string":"1987"},"artist":{"string":"U2"},"type":{"string":"Rock"}}
{"title":{"string":"Drowning Man"},"album":{"string":"War"},"duration":{"string":"04:14"},"release":{"string":"1983"},"artist":{"string":"U2"},"type":{"string":"Rock"}}
{"title":{"string":"Even Better Than the Real Thing"},"album":{"string":"Achtung Baby"},"duration":{"string":"03:41"},"release":{"string":"1991"},"artist":{"string":"U2"},"type":{"string":"Rock"}}
{"title":{"string":"Exit"},"album":{"string":"The Joshua Tree"},"duration":{"string":"04:13"},"release":{"string":"1987"},"artist":{"string":"U2"},"type":{"string":"Rock"}}
{"title":{"string":"In God's Country"},"album":{"string":"The Joshua Tree"},"duration":{"string":"02:56"},"release":{"string":"1987"},"artist":{"string":"U2"},"type":{"string":"Rock"}}
...
```
