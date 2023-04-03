---
date: 2022-03-02
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
* `AmazonS3BytesArrayInputReader`
* `AmazonS3RowFileInputReader`
* `AmazonS3XMLFileInputReader`
* `AmazonS3MetadataFileInputReader`

**Azure Blob Storage**

package: `io.streamthoughts.kafka.connect.filepulse.fs.reader`

* `AzureBlobStorageAvroFileInputReader`
* `AzureBlobStorageBytesArrayInputReader`
* `AzureBlobStorageRowFileInputReader`
* `AzureBlobStorageXMLFileInputReader`
* `AzureBlobStorageMetadataFileInputReader`

**Google Cloud Storage**

package: `io.streamthoughts.kafka.connect.filepulse.fs.reader`

* `GcsAvroFileInputReader`
* `GcsBytesArrayInputReader`
* `GcsRowFileInputReader`
* `GcsXMLFileInputReader`
* `GcsMetadataFileInputReader`

**Local Filesystem**

package: `io.streamthoughts.kafka.connect.filepulse.fs.reader`

* `LocalAvroFileInputReader`
* `LocalBytesArrayInputReader`
* `LocalRowFileInputReader`
* `LocalXMLFileInputReader`
* `LocalMetadataFileInputReader`

## RowFileInputReader (default)

The `<PREFIX>RowFileInputReader`s can be used to read files line by line.
This reader creates one record per row. It should be used for reading delimited text files, application log files, etc.

### Configuration

| Configuration               | Description                                                                        | Type      | Default | Importance |
|-----------------------------|------------------------------------------------------------------------------------|-----------|---------|------------|
| `file.encoding`             | The text file encoding to use                                                      | `String`  | `UTF_8` | High       | 
| `buffer.initial.bytes.size` | The initial buffer size used to read input files.                                  | `String`  | `4096`  | Medium     | 
| `min.read.records`          | The minimum number of records to read from file before returning to task.          | `Integer` | `1`     | Medium     | 
| `skip.headers`              | The number of rows to be skipped in the beginning of file.                         | `Integer` | `0`     | Medium     | 
| `skip.footers`              | The number of rows to be skipped at the end of file.                               | `Integer` | `0`     | Medium     | 
| `read.max.wait.ms`          | The maximum time to wait in milliseconds for more bytes after hitting end of file. | `Long`    | `0`     | Medium     | 

## XxxBytesArrayInputReader

The `<PREFIX>BytesArrayInputReader`s create a single byte array record from a source file.

## XxxAvroFileInputReader

The `<PREFIX>AvroFileInputReader`s can be used to read Avro files.

## XxxXMLFileInputReader

The `<PREFIX>XMLFileInputReader`s can be used to read XML files.

### Configuration

| Configuration                                         | Since   | Description                                                                                                                           | Type      | Default   | Importance |
|-------------------------------------------------------|---------|---------------------------------------------------------------------------------------------------------------------------------------|-----------|-----------|------------|
| `reader.xpath.expression`                             |         | The XPath expression used extract data from XML input files                                                                           | `String`  | `/`       | High       | 
| `reader.xpath.result.type`                            |         | The expected result type for the XPath expression in [NODESET, STRING]                                                                | `String`  | `NODESET` | High       | 
| `reader.xml.force.array.on.fields`                    |         | The comma-separated list of fields for which an array-type must be forced                                                             | `List`    | -         | High       |                                                
| `reader.xml.parser.validating.enabled`                | `2.2.0` | Specifies that the parser will validate documents as they are parsed.                                                                 | `boolean` | `false`   | Low        |
| `reader.xml.parser.namespace.aware.enabled`           | `2.2.0` | Specifies that the XML parser will provide support for XML namespaces.                                                                | `boolean` | `false`   | Low        |
| `reader.xml.exclude.empty.elements`                   | `2.2.0` | Specifies that the reader should exclude element having no field.                                                                     | `boolean` | `false`   | Low        |
| `reader.xml.exclude.node.attributes`                  | `2.4.0` | Specifies that the reader should exclude all node attributes.                                                                         | `boolean` | `false`   | Low        |
| `reader.xml.exclude.node.attributes.in.namespaces`    | `2.4.0` | Specifies that the reader should only exclude node attributes in the defined list of namespaces.                                      | `list`    | `false`   | Low        |
| `reader.xml.data.type.inference.enabled`              | `2.3.0` | Specifies that the reader should try to infer the type of data nodes.                                                                 | `boolean` | `false`   | High       |
| `reader.xml.attribute.prefix`                         | `2.4.0` | If set, the name of attributes will be prepended with the specified prefix when they are added to a record.                           | `string`  | `""`      | Low        |
| `reader.xml.content.field.name`                       | `2.5.4` | Specifies the name to be used for naming the field that will contain the value of a TextNode element having attributes.               | `string`  | `value`   | Low        |
| `reader.xml.field.name.characters.regex.pattern`      | `2.5.4` | Specifies the regex pattern to use for matching the characters in XML element name to replace when converting a document to a struct. | `string`  | `[.\-]'`  | Low        |
| `reader.xml.field.name.characters.string.replacement` | `2.5.4` | Specifies the replacement string to be used when converting a document to a struct.                                                   | `string`  | `_`       | Low        |
| `reader.xml.force.content.field.for.paths`            | `2.5.4` | The comma-separated list of field for which a content-field must be forced.                                                           | `List`    | -         | Low        |

## XxxMetadataFileInputReader

The `FileInputMetadataReader`s can be used to send a single record per file containing metadata, i.e.: `name`, `path`, `hash`, `lastModified`, `size`, etc.