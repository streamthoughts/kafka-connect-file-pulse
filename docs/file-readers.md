# File Readers

The connector can be configured with a specific [FileInputReader](https://github.com/streamthoughts/kafka-connect-file-pulse/blob/master/connect-file-pulse-api/src/main/java/io/streamthoughts/kafka/connect/filepulse/reader/FileInputReader.java).
The FileInputReader is used by tasks to read scheduled source files.

## RowFileInputReader (default)

The `RowFileInputReader` reads files from the local file system line by line.
This reader creates one record per row. It should be used for reading delimited text files, application log files, etc.

The following provides usage information for `io.streamthoughts.kafka.connect.filepulse.reader.RowFileInputReader` ([source code](https://github.com/streamthoughts/kafka-connect-file-pulse/blob/master/connect-file-pulse-plugin/src/main/java/io/streamthoughts/kafka/connect/filepulse/reader/RowFileInputReader.java))

## BytesArrayInputReader

The `BytesArrayInputReader` create a single byte array record from a source file.

The following provides usage information for `io.streamthoughts.kafka.connect.filepulse.reader.BytesArrayInputReader` ([source code](https://github.com/streamthoughts/kafka-connect-file-pulse/blob/master/connect-file-pulse-plugin/src/main/java/io/streamthoughts/kafka/connect/filepulse/reader/BytesArrayInputReader.java))

## AvroFileInputReader

The `AvroFileInputReader` is used to read Avro files.

The following provides usage information for `io.streamthoughts.kafka.connect.filepulse.reader.AvroFileInputReader` ([source code](https://github.com/streamthoughts/kafka-connect-file-pulse/blob/master/connect-file-pulse-plugin/src/main/java/io/streamthoughts/kafka/connect/filepulse/reader/AvroFileInputReader.java))

{% include_relative plan.md %}