---
date: 2020-05-22
title: "Getting Started"
linkTitle: "Getting Started"
weight: 10
description: >
  Get started with Connect Fie Pulse through a step by step tutorial.
---

In this tutorial we will explore how to deploy a basic Connect File Pulse connector step by step.

The prerequisites for this tutorial are :

* IDE or Text editor.
* Java 11
* Maven 3+
* Docker (for running a Kafka Cluster 2.x).

	
## Getting Started

### Installation

#### Download, Build & Package Connector

```bash
git clone https://github.com/streamthoughts/kafka-connect-file-pulse.git
cd kafka-connect-file-pulse
```

You can build kafka-connect-file-pulse with Maven using standard lifecycle.

```bash
mvn clean install -DskipTests
```


#### Install Connector Using Confluent Hub

Connector will be package into an archive file compatible with [confluent-hub client](https://docs.confluent.io/current/connect/managing/confluent-hub/client.html) :

```
./connect-file-pulse-plugin/target/components/packages/streamthoughts-kafka-connect-file-pulse-plugin-<FILEPULSE_VERSION>.zip
```


## Demonstrations

### Start Docker Environment

**1 ) Run Confluent Platforms with Connect File Pulse**

```bash
docker-compose build
docker-compose up -d
```

**2 ) Check for Kafka Connect**
```
docker logs --tail="all" -f connect"
```

**3 ) Verify that Connect File Pulse plugin is correctly loaded**
```bash
curl -sX GET http://localhost:8083/connector-plugins | grep FilePulseSourceConnector
```


### Example : Logs Parsing (Log4j)

This example starts a new connector instance to parse the Kafka Connect container log4j logs before writing them into a configured topic.


**1 ) Start a new connector instance**

```bash
curl -sX POST http://localhost:8083/connectors \
-d @config/connect-file-pulse-quickstart-log4j.json \
--header "Content-Type: application/json" | jq
```

**2 ) Check connector status**
```bash
curl -X GET http://localhost:8083/connectors/connect-file-pulse-quickstart-log4j | jq
```

**3 ) Consume output topics**
```bash
docker exec -it -e KAFKA_OPTS="" connect kafka-avro-console-consumer --topic connect-file-pulse-quickstart-log4j --from-beginning --bootstrap-server broker:29092 --property schema.registry.url=http://schema-registry:8081
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
docker exec -it -e KAFKA_OPTS="" connect kafka-console-consumer --topic connect-file-pulse-status --from-beginning --bootstrap-server broker:29092
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
curl -sX POST http://localhost:8083/connectors \
-d @config/connect-file-pulse-quickstart-csv.json \
--header "Content-Type: application/json" | jq
```

**2 ) Copy example csv file into container**

```bash
docker exec -it connect mkdir -p /tmp/kafka-connect/examples
docker cp examples/quickstart-musics-dataset.csv connect://tmp/kafka-connect/examples/quickstart-musics-dataset.csv
```

**3 ) Check connector status**
```bash
curl -X GET http://localhost:8083/connectors/connect-file-pulse-quickstart-csv | jq
```

**4 ) Check for task completion**
```
docker logs --tail="all" -f connect | grep "Orphan task detected"
```

**5 ) Consume output topics**
```bash
docker exec -it connect kafka-avro-console-consumer --topic connect-file-pulse-quickstart-csv --from-beginning --bootstrap-server broker:29092 --property schema.registry.url=http://schema-registry:8081
```
