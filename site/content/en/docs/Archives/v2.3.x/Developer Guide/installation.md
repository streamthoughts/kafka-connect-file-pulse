---
date: 2021-09-05
title: "Installation"
linkTitle: "Installation"
weight: 10
description: >
  How to install Connect File Pulse
---

## Confluent Hub

Connect FilePulse can be installed directly from [Confluent Hub](https://www.confluent.io/hub/streamthoughts/kafka-connect-file-pulse) using the [Confluent Hub client](https://docs.confluent.io/current/confluent-hub/client.html) .

The following command can be used to install the last version of the plugin available on the Confluent Hub:

```bash
$ confluent-hub install streamthoughts/kafka-connect-file-pulse:latest
```

{{% alert title="Caution" color="warning" %}}
You should note that the connector downloaded from Confluent Hub may not reflect the latest available version.
{{% /alert %}}

## Manual Installation

Connect FilePulse is distributed as a ZIP file which is compatible with the [Confluent Hub client](https://docs.confluent.io/current/confluent-hub/client.html).
All Connect FilePulse versions are available on the [GitHub Releases Page](https://github.com/streamthoughts/kafka-connect-file-pulse/releases).

To manually install the connector you can download the distribution ZIP file and extract all the dependencies under a target directory.

1. Download the latest available version: 
    ```bash
    $ export VERSION=2.3.0
    $ curl -sSL https://github.com/streamthoughts/kafka-connect-file-pulse/releases/download/v$VERSION/streamthoughts-kafka-connect-file-pulse-$VERSION.zip
    ```
2. Create a directory under the `plugin.path` on your Connect worker, e.g., `connect-source-filepulse`.
3. Copy all of the dependencies under the newly created subdirectory.
4. Restart the Connect worker.

Note: You can also use the [Confluent Hub CLI](https://docs.confluent.io/current/confluent-hub/client.html) for installing it.
```bash
$ confluent-hub install --no-prompt streamthoughts-kafka-connect-file-pulse-$VERSION.zip
```

{{% alert title="Important" color="info" %}}
When you run Connect workers in **distributed mode**, the connector-plugin must be installed **on each of machines** running Kafka Connect.
{{% /alert %}}



