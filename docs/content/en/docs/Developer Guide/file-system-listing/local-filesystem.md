---
date: 2023-09-12
title: "Local"
linkTitle: "Local"
weight: 10
description: >
  Learn how to configure the `LocalFSDirectoryListing` to read files from local filesystem.
---

The `LocalFSDirectoryListing` class can be used for listing files that exist in a local filesystem directory.

## How to use it ?

Use the following property in your Connector's configuration:

`fs.listing.class=io.streamthoughts.kafka.connect.filepulse.fs.LocalFSDirectoryListing`

## Configuration

The following table describes the properties that can be used to configure the `LocalFSDirectoryListing`:

| Configuration                      | Description                                                                | Type      | Default | Importance |
| ---------------------------------- | -------------------------------------------------------------------------- | --------- | ------- | ---------- |
| `fs.listing.directory.path`        | The input directory to scan                                                | `string`  | -       | HIGH       |
| `fs.listing.recursive.enabled`     | Flag indicating whether local directory should be recursively scanned      | `boolean` | `true`  | MEDIUM     |
| `fs.delete.compress.files.enabled` | Flag indicating whether compressed file should be deleted after extraction | `boolean` | `false` | MEDIUM     |

## Supported File types

The `LocalFSDirectoryListing` will try to detect if a file needs to be decompressed by probing its content type or its extension (javadoc : [Files#probeContentType](https://docs.oracle.com/javase/8/docs/api/java/nio/file/Files.html#probeContentType-java.nio.file.Path))
Supported content-types are:

* **GZIP** : `application/x-gzip`
* **TAR** : `application/x-tar`
* **ZIP** : `application/x-zip-compressed` or `application/zip`