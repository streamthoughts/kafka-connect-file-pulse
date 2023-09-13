---
date: 2023-09-12
title: "Azure Blob Storage"
linkTitle: "Azure Blob Storage"
weight: 40
description: >
  Learn how to configure the `AzureBlobStorageFileSystemListing` to read object files from Azure Blob Storage.
---

The `AzureBlobStorageConfig` class can be used for listing objects that exist in a specific Azure Storage Container.

## How to use it ?

Use the following property in your Connector's configuration:

`fs.listing.class=io.streamthoughts.kafka.connect.filepulse.fs.AzureBlobStorageFileSystemListing`

## Configuration

The following table describes the properties that can be used to configure the `AzureBlobStorageFileSystemListing`:

| Configuration                     | Description                                                                      | Type     | Default | Importance |
|-----------------------------------|----------------------------------------------------------------------------------|----------|---------|------------|
| `azure.storage.connection.string` | Azure storage account connection string.                                         | `string` | -       | HIGH       |
| `azure.storage.account.name`      | The Azure storage account name.                                                  | `string` | -       | HIGH       |
| `azure.storage.account.key`       | The Azure storage account key.                                                   | `string` | -       | HIGH       |
| `azure.storage.container.name`    | The Azure storage container name.                                                | `string` | -       | MEDIUM     |
| `azure.storage.blob.prefix`       | The prefix to be used for restricting the listing of the blobs in the container. | `string` | -       | MEDIUM     |                                                                                                                                                                      |          |         |            |