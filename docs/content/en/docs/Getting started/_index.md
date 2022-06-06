---
date: 2022-06-06
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
* Docker (for running a Kafka Cluster 3.x).

## Start Docker Environment

Set the following environment variable to execute next commands.

```bash
export GITHUB_REPO_MASTER=https://raw.githubusercontent.com/streamthoughts/kafka-connect-file-pulse/master/
```

**1 ) Run Confluent Platforms with Connect File Pulse**

```bash
curl -sSL $GITHUB_REPO_MASTER/docker-compose.yml -o docker-compose.yml
docker-compose up -d
```

**2 ) Verify that Connect Worker is running (optional)**

```bash
docker-compose logs "connect-file-pulse"
```

**3 ) Check that Connect File Pulse plugin is correctly loaded (optional)**

```bash
curl -sX GET http://localhost:8083/connector-plugins | grep FilePulseSourceConnector
```

## Examples 

### Logs Parsing (Log4j)

This example starts a new connector instance to parse the Kafka Connect container log4j logs before writing them into a configured topic.

**1 ) Start a new connector instance**

```bash
 
curl -sSL $GITHUB_REPO_MASTER/examples/connect-file-pulse-quickstart-log4j.json -o connect-file-pulse-quickstart-log4j.json
 
$ curl -sX PUT http://localhost:8083/connectors/connect-file-pulse-quickstart-log4j/config \
-d @connect-file-pulse-quickstart-log4j.json \
--header "Content-Type: application/json" | jq
```

**2 ) Check connector status**
```bash
curl -X GET http://localhost:8083/connectors/connect-file-pulse-quickstart-log4j/status | jq
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
{"logdate":{"string":"2022-06-06 14:10:34,193"},"loglevel":{"string":"INFO"},"message":{"string":"[task-thread-connect-file-pulse-quickstart-log4j-0] Started FilePulse source task (io.streamthoughts.kafka.connect.filepulse.source.FilePulseSourceTask)"}}
{"logdate":{"string":"2022-06-06 14:10:34,193"},"loglevel":{"string":"INFO"},"message":{"string":"[task-thread-connect-file-pulse-quickstart-log4j-0] WorkerSourceTask{id=connect-file-pulse-quickstart-log4j-0} Source task finished initialization and start (org.apache.kafka.connect.runtime.WorkerSourceTask)"}}
{"logdate":{"string":"2022-06-06 14:10:34,194"},"loglevel":{"string":"INFO"},"message":{"string":"[task-thread-connect-file-pulse-quickstart-log4j-0] WorkerSourceTask{id=connect-file-pulse-quickstart-log4j-0} Executing source task (org.apache.kafka.connect.runtime.WorkerSourceTask)"}}
{"logdate":{"string":"2022-06-06 14:10:34,695"},"loglevel":{"string":"INFO"},"message":{"string":"[task-thread-connect-file-pulse-quickstart-log4j-0] Completed all object files. FilePulse source task is transitioning to IDLE state while waiting for new reconfiguration request from source connector. (io.streamthoughts.kafka.connect.filepulse.source.FilePulseSourceTask)"}}
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
{"metadata":{"uri":"file:/var/log/kafka/kafka-connect.log","name":"kafka-connect.log","contentLength":110900,"lastModified":1654524649569,"contentDigest":{"digest":"2445040665","algorithm":"CRC32"},"userDefinedMetadata":{"system.inode":33827724,"system.hostname":"5c1e920f9a28"}},"offset":{"position":-1,"rows":0,"timestamp":1654524649578},"status":"SCHEDULED"}
{"metadata":{"uri":"file:/var/log/kafka/kafka-connect.log","name":"kafka-connect.log","contentLength":111122,"lastModified":1654524649597,"contentDigest":{"digest":"1755089447","algorithm":"CRC32"},"userDefinedMetadata":{"system.inode":33827724,"system.hostname":"5c1e920f9a28"}},"offset":{"position":0,"rows":0,"timestamp":1654524649604},"status":"STARTED"}
{"metadata":{"uri":"file:/var/log/kafka/kafka-connect.log","name":"kafka-connect.log","contentLength":111122,"lastModified":1654524649597,"contentDigest":{"digest":"1755089447","algorithm":"CRC32"},"userDefinedMetadata":{"system.inode":33827724,"system.hostname":"5c1e920f9a28"}},"offset":{"position":111530,"rows":1271,"timestamp":1654524654094},"status":"READING"}
{"metadata":{"uri":"file:/var/log/kafka/kafka-connect.log","name":"kafka-connect.log","contentLength":111122,"lastModified":1654524649597,"contentDigest":{"digest":"1755089447","algorithm":"CRC32"},"userDefinedMetadata":{"system.inode":33827724,"system.hostname":"5c1e920f9a28"}},"offset":{"position":112158,"rows":1274,"timestamp":1654524664011},"status":"READING"}
{"metadata":{"uri":"file:/var/log/kafka/kafka-connect.log","name":"kafka-connect.log","contentLength":111122,"lastModified":1654524649597,"contentDigest":{"digest":"1755089447","algorithm":"CRC32"},"userDefinedMetadata":{"system.inode":33827724,"system.hostname":"5c1e920f9a28"}},"offset":{"position":112786,"rows":1277,"timestamp":1654524674029},"status":"READING"}
```

**5 ) Stop all containers**
```bash
docker-compose down
```

### Parsing a CSV file

This example starts a new connector instance that parse a CSV file and filter rows based on column's values before writing record into Kafka.

**1 ) Start a new connector instance**

```bash
$ curl -sSL $GITHUB_REPO_MASTER/examples/connect-file-pulse-quickstart-csv.json -o connect-file-pulse-quickstart-csv.json

$ curl -sX PUT http://localhost:8083/connectors/connect-file-pulse-quickstart-csv/config \
-d @connect-file-pulse-quickstart-csv.json \
--header "Content-Type: application/json" | jq
```

**2 ) Copy example csv file into container**

```bash
$ curl -sSL $GITHUB_REPO_MASTER/datasets/quickstart-musics-dataset.csv -o quickstart-musics-dataset.csv
$ docker exec -it connect mkdir -p /tmp/kafka-connect/examples
$ docker cp quickstart-musics-dataset.csv connect://tmp/kafka-connect/examples/quickstart-musics-dataset.csv
```

**3 ) Check connector status**
```bash
$ curl -X GET http://localhost:8083/connectors/connect-file-pulse-quickstart-csv/status | jq
```

**4 ) Check for task completion**

```
docker logs --tail="all" -f connect | grep "source task is transitioning to IDLE state"
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
