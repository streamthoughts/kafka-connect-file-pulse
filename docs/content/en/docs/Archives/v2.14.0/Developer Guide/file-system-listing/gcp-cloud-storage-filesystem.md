---
date: 2023-09-12
title: "GCP Cloud Storage"
linkTitle: "GCP Cloud Storage"
weight: 30
description: >
  Learn how to configure the `GcsFileSystemListing` to read files from Google Cloud Storage.
---

The `GcsFileSystemListing` class can be used for listing objects that exist in a specific Google Cloud Storage bucket.

## How to use it ?

Use the following property in your Connector's configuration:

`fs.listing.class=io.streamthoughts.kafka.connect.filepulse.fs.GcsFileSystemListing`

## Configuration

The following table describes the properties that can be used to configure the `GcsFileSystemListing`:

| Configuration             | Description                                                                                                                                                                                                                                  | Type     | Default | Importance |
|---------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|---------|------------|
| `gcs.credentials.path`    | The path to GCP credentials file. Cannot be set when `GCS_CREDENTIALS_JSON_CONFIG` is provided. If no credentials is specified the client library will look for credentials via the environment variable `GOOGLE_APPLICATION_CREDENTIALS`.   | `string` | -       | HIGH       |
| `gcs.credentials.json`    | The GCP credentials as JSON string. Cannot be set when `GCS_CREDENTIALS_PATH_CONFIG` is provided. If no credentials is specified the client library will look for credentials via the environment variable `GOOGLE_APPLICATION_CREDENTIALS`. | `string` | -       | HIGH       |
| `gcs.bucket.name`         | The GCS bucket name to download the object files from.                                                                                                                                                                                       | `string` | -       | HIGH       |
| `gcs.blobs.filter.prefix` | The prefix to be used for filtering blobs  whose names begin with it.                                                                                                                                                                        | `string` | -       | MEDIUM     |