---
date: 2023-09-12
title: "AWS S3"
linkTitle: "AWS S3"
weight: 20
description: >
  Learn how to configure the `AmazonS3FileSystemListing` to read files from AWS S3.
---

The `AmazonS3FileSystemListing` class can be used for listing objects that exist in a specific Amazon S3 bucket.

## How to use it ?

Use the following property in your Connector's configuration:

`fs.listing.class=io.streamthoughts.kafka.connect.filepulse.fs.AmazonS3FileSystemListing`

## Configuration

The following table describes the properties that can be used to configure the `AmazonS3FileSystemListing`:

| Configuration                         | Description                                                                                                                                                                                                                                                | Type     | Default                                                     | Importance |
|---------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|-------------------------------------------------------------|------------|
| `aws.access.key.id`                   | AWS Access Key ID AWS                                                                                                                                                                                                                                      | `string` | -                                                           | HIGH       |
| `aws.secret.access.key`               | AWS Secret Access Key                                                                                                                                                                                                                                      | `string` | -                                                           | HIGH       |
| `aws.secret.session.token`            | AWS Secret Session Token                                                                                                                                                                                                                                   | `string` | -                                                           | HIGH       |
| `aws.credentials.provider.class`      | The AWSCredentialsProvider to use if no access key id and secret access key is configured.                                                                                                                                                                 | `class`  | `com.amazonaws.auth.EnvironmentVariableCredentialsProvider` | LOW        |
| `aws.s3.region`                       | The AWS S3 Region, e.g. us-east-1                                                                                                                                                                                                                          | `string` | `Regions.DEFAULT_REGION.getName()`                          | MEDIUM     |
| `aws.s3.service.endpoint`             | AWS S3 custom service endpoint.                                                                                                                                                                                                                            | `string` | -                                                           | MEDIUM     |
| `aws.s3.path.style.access.enabled`    | Configures the client to use path-style access for all requests.                                                                                                                                                                                           | `string` | -                                                           | MEDIUM     |
| `aws.s3.bucket.name`                  | The name of the Amazon S3 bucket.                                                                                                                                                                                                                          | `string` | -                                                           | HIGH       |
| `aws.s3.bucket.prefix`                | The prefix to be used for restricting the listing of the objects in the bucket                                                                                                                                                                             | `string` | -                                                           | MEDIUM     |
| `aws.s3.default.object.storage.class` | The AWS storage class to associate with an S3 object when it is copied by the connector (e.g., during a move operation). Accepted values are: `STANDARD`, `GLACIER`, `REDUCED_REDUNDANCY`, `STANDARD_IA`,`ONEZONE_IA`,`INTELLIGENT_TIERING`,`DEEP_ARCHIVE` | `string` |                                                             | LOW        |
| `aws.s3.backoff.delay.ms`             | The base back-off time (milliseconds) before retrying a request.                                                                                                                                                                                           | `int`    | `100`                                                       | MEDIUM     |
| `aws.s3.backoff.max.delay.ms`         | The maximum back-off time (in milliseconds) before retrying a request.                                                                                                                                                                                     | `int`    | `20_000`                                                    | MEDIUM     |
| `aws.s3.backoff.max.retries`          | The maximum number of retry attempts for failed retryable requests.                                                                                                                                                                                        | `int`    | `3                                                          | MEDIUM     |