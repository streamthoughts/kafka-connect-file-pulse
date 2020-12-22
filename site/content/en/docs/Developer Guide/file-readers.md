---
date: 2020-08-12
title: "File Readers"
linkTitle: "File Readers"
weight: 40
description: >
  The commons configuration for Connect File Pulse.
---

The connector can be configured with a specific [FileInputReader](https://github.com/streamthoughts/kafka-connect-file-pulse/blob/master/connect-file-pulse-api/src/main/java/io/streamthoughts/kafka/connect/filepulse/reader/FileInputReader.java).
The FileInputReader is used by tasks to read scheduled source files.

## RowFileInputReader (default)

The `RowFileInputReader` reads files from the local file system line by line.
This reader creates one record per row. It should be used for reading delimited text files, application log files, etc.

The following provides usage information for `io.streamthoughts.kafka.connect.filepulse.reader.RowFileInputReader` ([source code](https://github.com/streamthoughts/kafka-connect-file-pulse/blob/master/connect-file-pulse-plugin/src/main/java/io/streamthoughts/kafka/connect/filepulse/reader/RowFileInputReader.java))

### Configuration

| Configuration |   Description |   Type    |   Default |   Importance  |
| --------------| --------------|-----------| --------- | ------------- |
|`file.encoding` | The text file encoding to use | `String` | `UTF_8` | High | 
|`buffer.initial.bytes.size` | The initial buffer size used to read input files. | `String` | `4096` | Medium | 
|`min.read.records` | The minimum number of records to read from file before returning to task. | `Integer` | `1` | Medium | 
|`skip.headers` | The number of rows to be skipped in the beginning of file. | `Integer` | `0` | Medium | 
|`skip.footers` | The number of rows to be skipped at the end of file. | `Integer` | `0` | Medium | 
|`read.max.wait.ms` | The maximum time to wait in milliseconds for more bytes after hitting end of file. | `Long` | `0` | Medium | 

## BytesArrayInputReader

The `BytesArrayInputReader` create a single byte array record from a source file.

The following provides usage information for `io.streamthoughts.kafka.connect.filepulse.reader.BytesArrayInputReader` ([source code](https://github.com/streamthoughts/kafka-connect-file-pulse/blob/master/connect-file-pulse-plugin/src/main/java/io/streamthoughts/kafka/connect/filepulse/reader/BytesArrayInputReader.java))

## AvroFileInputReader

The `AvroFileInputReader` is used to read Avro files.

The following provides usage information for `io.streamthoughts.kafka.connect.filepulse.reader.AvroFileInputReader` ([source code](https://github.com/streamthoughts/kafka-connect-file-pulse/blob/master/connect-file-pulse-plugin/src/main/java/io/streamthoughts/kafka/connect/filepulse/reader/AvroFileInputReader.java))

## XMLFileInputReader

The `XMLFileInputReader` is used to read XML files.

The following provides usage information for `io.streamthoughts.kafka.connect.filepulse.reader.XMLFileInputReader` ([source code](https://github.com/streamthoughts/kafka-connect-file-pulse/blob/master/connect-file-pulse-plugin/src/main/java/io/streamthoughts/kafka/connect/filepulse/reader/XMLFileInputReader.java))

### Configuration

| Configuration |   Description |   Type    |   Default |   Importance  |
| --------------| --------------|-----------| --------- | ------------- |
|`xpath.expression` | The XPath expression used extract data from XML input files | `String` | `/` | High | 
|`xpath.result.type` | The expected result type for the XPath expression in [NODESET, STRING] | `String` | `NODESET` | High | 
|`force.array.on.fields` | The comma-separated list of fields for which an array-type must be forced | `List` | `-` | High |                                                

## FileInputMetadataReader

The `FileInputMetadataReader` is used to send a single record per file containing metadata (i.e: `name`, `path`, `hash`, `lastModified`, `size`, etc)

The following provides usage information for `io.streamthoughts.kafka.connect.filepulse.reader.FileInputMetadataReader` ([source code](https://github.com/streamthoughts/kafka-connect-file-pulse/blob/master/connect-file-pulse-plugin/src/main/java/io/streamthoughts/kafka/connect/filepulse/reader/FileInputMetadataReader.java))