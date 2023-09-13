---
date: 2023-09-12
title: "Alibaba OSS"
linkTitle: "Alibaba OSS"
weight: 60
description: >
  Learn how to configure the `AliyunOSSFileSystemListing` to read files on Alibaba OSS.
---

The `AliyunOSSFileSystemListing` class can be used for listing files on Alibaba Cloud Object Storage Service.

## How to use it ?

Use the following property in your Connector's configuration:

`fs.listing.class=io.streamthoughts.kafka.connect.filepulse.fs.AliyunOSSFileSystemListing`

## Configuration

The following table describes the properties that can be used to configure the `AliyunOSSFileSystemListing`:

| Configuration            | Description                                                         | Type     | Default | Importance |
|--------------------------|---------------------------------------------------------------------|----------|---------|------------|
| `oss.access.key.id`      | OSS access key id                                                   | `string` | -       | HIGH       |
| `oss.secret.key`         | OSS secret key                                                      | `int`    | -       | HIGH       |
| `oss.endpoint`           | OSS access endpoint                                                 | `string` | -       | HIGH       |
| `oss.bucket.name`        | OSS bucket name                                                     | `string` | -       | HIGH       |
| `oss.bucket.prefix`      | OSS bucket prefix                                                   | `string` | -       | HIGH       |
| `oss.max.connections`    | OSS max connections.                                                | `int`    | `1024`  | HIGH       |
| `oss.socket.timeout`     | OSS connection timeout (in milliseconds).                           | `int`    | `10000` | HIGH       |
| `oss.connection.timeout` | OSS connection timeout (in milliseconds).                           | `int`    | `50000` | HIGH       |
| `oss.max.error.retries`  | The maximum number of retry attempts for failed retryable requests. | `int`    | `5`     | HIGH       |
