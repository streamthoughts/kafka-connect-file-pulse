---
date: 2022-04-13
title: "FileSystem Listing"
linkTitle: "FileSystem Listing"
weight: 30
description: >
  Learn how to configure Connect FilePulse for listing files from local or remote storage system.
---

The `FilePulseSourceConnector` periodically lists object files that may be streamed into Kafka using the [FileSystemListing](https://github.com/streamthoughts/kafka-connect-file-pulse/blob/master/connect-file-pulse-api/src/main/java/io/streamthoughts/kafka/connect/filepulse/fs/FileSystemListing.java)  
configured in the connector's configuration.

## Supported Filesystems

Currently, Kafka Connect FilePulse supports the following implementations: 

* `AmazonS3FileSystemListing`
* `AzureBlobStorageFileSystemListing`
* `GcsFileSystemListing`
* `LocalFSDirectoryListing` (default)

### Local Filesystem (default)

The `LocalFSDirectoryListing` class can be used for listing files that exist in a local filesystem directory.

#### How to use it ?

`fs.listing.class=io.streamthoughts.kafka.connect.filepulse.fs.LocalFSDirectoryListing`

#### Configuration

| Configuration                  | Description                                                           | Type      | Default | Importance |
|--------------------------------|-----------------------------------------------------------------------|-----------|---------|------------|
| `fs.listing.directory.path`    | The input directory to scan                                           | `string`  | -       | HIGH       |
| `fs.listing.recursive.enabled` | Flag indicating whether local directory should be recursively scanned | `boolean` | `true`  | MEDIUM     |

#### Supported File types

The `LocalFSDirectoryListing` will try to detect if a file needs to be decompressed by probing its content type or its extension (javadoc : [Files#probeContentType](https://docs.oracle.com/javase/8/docs/api/java/nio/file/Files.html#probeContentType-java.nio.file.Path))
Supported content-types are:

* **GZIP** : `application/x-gzip`
* **TAR** : `application/x-tar`
* **ZIP** : `application/x-zip-compressed` or `application/zip`

### Amazon S3

The `AmazonS3FileSystemListing` class can be used for listing objects that exist in a specific Amazon S3 bucket.

#### How to use it ?

`fs.listing.class=io.streamthoughts.kafka.connect.filepulse.fs.AmazonS3FileSystemListing`

#### Configuration

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
| `aws.s3.backoff.max.retries`          | The maximum number of retry attempts for failed retryable requests.                                                                                                                                                                                        | `int`    | `3`                                                         | MEDIUM     |
    
### Google Cloud Storage

The `GcsFileSystemListing` class can be used for listing objects that exist in a specific Google Cloud Storage bucket.

#### How to use it ?

`fs.listing.class=io.streamthoughts.kafka.connect.filepulse.fs.GcsFileSystemListing`

#### Configuration

| Configuration             | Description                                                                                                                                                                                                                                  | Type     | Default | Importance |
|---------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|---------|------------|
| `gcs.credentials.path`    | The path to GCP credentials file. Cannot be set when `GCS_CREDENTIALS_JSON_CONFIG` is provided. If no credentials is specified the client library will look for credentials via the environment variable `GOOGLE_APPLICATION_CREDENTIALS`.   | `string` | -       | HIGH       |
| `gcs.credentials.json`    | The GCP credentials as JSON string. Cannot be set when `GCS_CREDENTIALS_PATH_CONFIG` is provided. If no credentials is specified the client library will look for credentials via the environment variable `GOOGLE_APPLICATION_CREDENTIALS`. | `string` | -       | HIGH       |
| `gcs.bucket.name`         | The GCS bucket name to download the object files from.                                                                                                                                                                                       | `string` | -       | HIGH       |
| `gcs.blobs.filter.prefix` | The prefix to be used for filtering blobs  whose names begin with it.                                                                                                                                                                        | `string` | -       | MEDIUM     |

### Azure Blob Storage

The `AzureBlobStorageConfig` class can be used for listing objects that exist in a specific Azure Storage Container.

#### How to use it ?

`fs.listing.class=io.streamthoughts.kafka.connect.filepulse.fs.AzureBlobStorageFileSystemListing`

#### Configuration

| Configuration                     | Description                                                                      | Type     | Default | Importance |
|-----------------------------------|----------------------------------------------------------------------------------|----------|---------|------------|
| `azure.storage.connection.string` | Azure storage account connection string.                                         | `string` | -       | HIGH       |
| `azure.storage.account.name`      | The Azure storage account name.                                                  | `string` | -       | HIGH       |
| `azure.storage.account.key`       | The Azure storage account key.                                                   | `string` | -       | HIGH       |
| `azure.storage.container.name`    | The Azure storage container name.                                                | `string` | -       | MEDIUM     |
| `azure.storage.blob.prefix`       | The prefix to be used for restricting the listing of the blobs in the container. | `string` | -       | MEDIUM     |

## Filtering input files

You can configure one or more `FileFilter` that will be used to determine if a file should be scheduled for processing or ignored. 
All files that are filtered out are simply ignored and remain untouched on the file system until the next scan. 
At the next scan, previously filtered files will be evaluated again to determine if they are now eligible for processing.

FilePulse packs with the following built-in filters :

### IgnoreHiddenFileFilter

The `IgnoreHiddenFileFilter` can be used to filter hidden files from being read.

**Configuration example**

```properties
fs.listing.filters=io.streamthoughts.kafka.connect.filepulse.fs.filter.IgnoreHiddenFileListFilter
```

{{% alert title="Limitation" color="warning" %}}
This filter is only supported by the `LocalFSDirectoryListing`.
{{% /alert %}}

### LastModifiedFileFilter

The `LastModifiedFileFilter` can be used to filter all files that have been modified to recently based on their last modified date property.

```properties
fs.listing.filters=io.streamthoughts.kafka.connect.filepulse.fs.filter.LastModifiedFileFilter
# The last modified time for a file can be accepted (default: 5000)
file.filter.minimum.age.ms=10000
```

### RegexFileFilter

The `RegexFileFilter` can be used to filter all files that do not match the specified regex.

```properties
fs.listing.filters=io.streamthoughts.kafka.connect.filepulse.fs.filter.RegexFileListFilter
# The regex pattern used to match input files
file.filter.regex.pattern="\\.log$"
```

### SizeFileListFilter

The `SizeFileListFilter` can be used to filter all files that are smaller or larger than a specific byte size.

```properties
fs.listing.filters=io.streamthoughts.kafka.connect.filepulse.fs.filter.RegexFileListFilter
file.filter.minimum.size.bytes=0
file.filter.maximum.size.bytes=9223372036854775807
```