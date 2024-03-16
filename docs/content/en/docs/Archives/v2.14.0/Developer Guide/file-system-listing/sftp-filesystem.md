---
date: 2023-09-12
title: "SFTP"
linkTitle: "SFTP"
weight: 15
description: >
  Learn how to configure the `SftpFilesystemListing` to read files on SFTP.
---

The `SftpFilesystemListing` class can be used for listing files on SFTP.

## How to use it ?

Use the following property in your Connector's configuration:

`fs.listing.class=io.streamthoughts.kafka.connect.filepulse.fs.SftpFilesystemListing`

## Configuration

The following table describes the properties that can be used to configure the `SftpFilesystemListing`:

| Configuration                        | Description                                     | Type     | Default | Importance |
|--------------------------------------|-------------------------------------------------|----------|---------|------------|
| `sftp.listing.host`                  | Hostname to connect to                          | `string` | -       | HIGH       |
| `sftp.listing.port`                  | Port to connect to                              | `int`    | `21`    | HIGH       |
| `sftp.listing.user`                  | SFTP username                                   | `string` | -       | HIGH       |
| `sftp.listing.password`              | SFTP user credentials                           | `string` | -       | HIGH       |
| `sftp.listing.directory.path`        | The input directory to scan                     | `string` | -       | HIGH       |
| `sftp.listing.strict.host.key.check` | String host key checking                        | `string` | `no`    | HIGH       |
| `sftp.connection.timeout`            | SFTP connection timeout in millis               | `string` | -       | HIGH       |
| `sftp.connection.retries`            | SFTP connection retries                         | `int`    | `5`     | HIGH       |
| `sftp.connection.retries.delay`      | SFTP connection delay between retries in millis | `int`    | `5000`  | HIGH       |
