# File Readers

The connector can be configured with a specific FileInputReader.
Currently, it only supports the `RowFileInputReader` that will read a file from the local file system line by line.

## RowFileInputReader

The following provides usage information for [FileInputReader](blob/master/src/main/java/io/streamthoughts/kafka/connect/filepulse/reader/FileInputReader.java)
:  `io.streamthoughts.kafka.connect.filepulse.reader.impl.RowFileInputReader` ([source code](blob/master/src/main/java/io/streamthoughts/kafka/connect/filepulse/reader/RowFileInputReader.java))

{% include_relative plan.md %}