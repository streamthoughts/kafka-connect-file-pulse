---
date: 2023-09-13
title: "FileListFilter"
linkTitle: "FileListFilter"
weight: 35
---

A `FileListFilter` allows to filter files that can be processed by the connector. Files that are filtered out are simply
ignored and remain untouched on the file system until the next file listing operation. During the next execution, the
previously filtered files will be evaluated again to determine whether they should be processed.

You can configure multiple `FileListFilter`s using the following connector's configuration property:

| Configuration                                  | Description                                                                         | Type    | Default                                                                   | 
|------------------------------------------------|-------------------------------------------------------------------------------------|---------|---------------------------------------------------------------------------|
| `fs.listing.filters`                           | A comma-separated list of FileListFilter classes used to list eligible input files. | list    | *-*                                                                       |

FilePulse provides several built-in `FileListFilter`:

## `IgnoreHiddenFileFilter`

You can use the `IgnoreHiddenFileFilter` to ignore hidden files.

**Configuration example**

```properties
fs.listing.filters=io.streamthoughts.kafka.connect.filepulse.fs.filter.IgnoreHiddenFileListFilter
```

{{% alert title="Limitation" color="warning" %}}
This `IgnoreHiddenFileFilter` can only be used when the `LocalFSDirectoryListing` is configured.
{{% /alert %}}

## `LastModifiedFileFilter`

You can use the `LastModifiedFileFilter` to filter only the files that have not been modified since a given duration.

```properties
fs.listing.filters=io.streamthoughts.kafka.connect.filepulse.fs.filter.LastModifiedFileFilter
# The last modified time for a file can be accepted (default: 5000)
file.filter.minimum.age.ms=10000
```

## `RegexFileFilter`

You can use the `RegexFileFilter` to filter files that match a given regular expression.

```properties
fs.listing.filters=io.streamthoughts.kafka.connect.filepulse.fs.filter.RegexFileListFilter
# The regex pattern used to match input files
file.filter.regex.pattern="\\.log$"
```

## `SizeFileListFilter`

You can use the `SizeFileListFilter` to filter files that are smaller or larger than a specific byte size.

```properties
fs.listing.filters=io.streamthoughts.kafka.connect.filepulse.fs.filter.RegexFileListFilter
file.filter.minimum.size.bytes=0
file.filter.maximum.size.bytes=9223372036854775807
```

## `DateInFilenameFileListFilter`

You can use the `DateInFilenameFileListFilter`to filter files that contain a date in filename earlier or later than
a specific date.

```properties
fs.listing.filters=io.streamthoughts.kafka.connect.filepulse.fs.filter.DateInFilenameFileListFilter
file.filter.date.regex.extractor.pattern="^.*(\\d{4}-\\d{2}-\\d{2})_.*$"
file.filter.date.formatter.pattern="yyyy MM dd"
file.filter.date.min.date="2023-08-23"
file.filter.date.max.date="2023-08-24"
```