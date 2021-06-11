---
date: 2021-06-10
title: "File Readers"
linkTitle: "File Readers"
weight: 40
description: >
  Learn how to configure Connect FilePulse for a specific file format.
---

The `FilePulseSourceTask` uses the [FileInputReader](https://github.com/streamthoughts/kafka-connect-file-pulse/blob/master/connect-file-pulse-api/src/main/java/io/streamthoughts/kafka/connect/filepulse/reader/FileInputReader.java).
configured in the connector's configuration for reading object files (i.e., `tasks.reader.class`).

Currently, Connect FilePulse provides the following `FileInputReader` implementations : 

**Amazon S3**

package: `io.streamthoughts.kafka.connect.filepulse.fs.reader`

* `AmazonS3AvroFileInputReader`
* `AmazonS3BytesFileInputReader`
* `AmazonS3RowFileInputReader`
* `AmazonS3XMLFileInputReader`
* `AmazonS3MetadataFileInputReader`

**Azure Blob Storage**

package: `io.streamthoughts.kafka.connect.filepulse.fs.reader`

* `AzureBlobStorageAvroFileInputReader`
* `AzureBlobStorageBytesFileInputReader`
* `AzureBlobStorageRowFileInputReader`
* `AzureBlobStorageXMLFileInputReader`
* `AzureBlobStorageMetadataFileInputReader`

**Google Cloud Storage**

package: `io.streamthoughts.kafka.connect.filepulse.fs.reader`

* `GcsAvroFileInputReader`
* `GcsBytesFileInputReader`
* `GcsRowFileInputReader`
* `GcsXMLFileInputReader`
* `GcsMetadataFileInputReader`

**Local Filesystem**

package: `io.streamthoughts.kafka.connect.filepulse.fs.reader`

* `LocalAvroFileInputReader`
* `LocalBytesFileInputReader`
* `LocalRowFileInputReader`
* `LocalXMLFileInputReader`
* `LocalMetadataFileInputReader`

## RowFileInputReader (default)

The `<PREFIX>RowFileInputReader`s can be used to read files line by line.
This reader creates one record per row. It should be used for reading delimited text files, application log files, etc.

### Configuration

| Configuration |   Description |   Type    |   Default |   Importance  |
| --------------| --------------|-----------| --------- | ------------- |
|`file.encoding` | The text file encoding to use | `String` | `UTF_8` | High | 
|`buffer.initial.bytes.size` | The initial buffer size used to read input files. | `String` | `4096` | Medium | 
|`min.read.records` | The minimum number of records to read from file before returning to task. | `Integer` | `1` | Medium | 
|`skip.headers` | The number of rows to be skipped in the beginning of file. | `Integer` | `0` | Medium | 
|`skip.footers` | The number of rows to be skipped at the end of file. | `Integer` | `0` | Medium | 
|`read.max.wait.ms` | The maximum time to wait in milliseconds for more bytes after hitting end of file. | `Long` | `0` | Medium | 

## XxxBytesArrayInputReader

The `<PREFIX>BytesArrayInputReader`s create a single byte array record from a source file.

## XxxAvroFileInputReader

The `<PREFIX>AvroFileInputReader`s can be used to read Avro files.

## XxxXMLFileInputReader

The `<PREFIX>XMLFileInputReader`s can be used to read XML files.

### Configuration

| Configuration |   Description |   Type    |   Default |   Importance  |
| --------------| --------------|-----------| --------- | ------------- |
|`xpath.expression` | The XPath expression used extract data from XML input files | `String` | `/` | High | 
|`xpath.result.type` | The expected result type for the XPath expression in [NODESET, STRING] | `String` | `NODESET` | High | 
|`force.array.on.fields` | The comma-separated list of fields for which an array-type must be forced | `List` | `-` | High |                                                

## XxxMetadataFileInputReader

The `FileInputMetadataReader`s can be used to send a single record per file containing metadata, i.e.: `name`, `path`, `hash`, `lastModified`, `size`, etc.