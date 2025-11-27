---
date: 2025-11-17
title: "SMB/CIFS"
linkTitle: "SMB/CIFS"
weight: 20
description: >
  Learn how to configure the `SmbFilesystemListing` to read files from SMB 3.x network shares.
---

The `SmbFilesystemListing` class can be used for listing files on SMB 3.x network shares (Windows file shares or Samba 4.x).

{{% alert title="Security Notice" color="warning" %}}
This implementation supports **SMB 3.0, 3.0.2, and 3.1.1 only**. SMB 1.0 and 2.x are intentionally disabled for security reasons. Ensure your file servers support SMB 3.0 or later.
{{% /alert %}}

## How to use it ?

Use the following property in your Connector's configuration:

`fs.listing.class=io.streamthoughts.kafka.connect.filepulse.fs.SmbFileSystemListing`

## Configuration

The following table describes the properties that can be used to configure the `SmbFilesystemListing`:

| Configuration                    | Description                                                      | Type      | Default                               | Importance |
|----------------------------------|------------------------------------------------------------------|-----------|---------------------------------------|------------|
| `smb.listing.host`               | SMB server hostname or IP address                                | `string`  | -                                     | HIGH       |
| `smb.listing.port`               | SMB server port                                                  | `int`     | `445`                                 | HIGH       |
| `smb.listing.share`              | SMB share name                                                   | `string`  | -                                     | HIGH       |
| `smb.listing.domain`             | SMB domain/workgroup                                             | `string`  | `WORKGROUP`                           | MEDIUM     |
| `smb.listing.user`               | SMB username                                                     | `string`  | -                                     | HIGH       |
| `smb.listing.password`           | SMB password                                                     | `string`  | -                                     | HIGH       |
| `smb.listing.directory.path`     | The directory path on the SMB share to scan                      | `string`  | `/`                                   | HIGH       |
| `smb.connection.timeout`         | SMB connection timeout in milliseconds (jcifs.smb.client.connTimeout) | `int`     | `30000`                               | MEDIUM     |
| `smb.response.timeout`           | SMB response timeout in milliseconds (jcifs.smb.client.responseTimeout) | `int`     | `30000`                               | MEDIUM     |
| `smb.so.timeout`                 | SMB socket timeout in milliseconds (jcifs.smb.client.soTimeout)  | `int`     | `30000`                               | MEDIUM     |
| `smb.connection.retries`         | Number of retries for SMB connection failures                    | `int`     | `5`                                   | MEDIUM     |
| `smb.connection.retries.delay`   | Delay between connection retries in milliseconds                 | `int`     | `5000`                                | MEDIUM     |
| `smb.protocol.max.version`       | Maximum SMB protocol version (SMB300, SMB302, SMB311)            | `string`  | `SMB311`                              | MEDIUM     |

{{% alert title="Note" color="info" %}}
**SMB Protocol Version**: The minimum version is hardcoded to SMB 3.0 for security. The maximum version is configurable and defaults to SMB 3.1.1 (SMB311). You can set it to SMB300 or SMB302 if needed for compatibility with older servers.

**Timeout Configuration**: Three separate timeout values can be configured:
- `smb.connection.timeout` - Timeout for establishing the initial connection
- `smb.response.timeout` - Timeout for receiving responses from the server
- `smb.so.timeout` - Socket timeout for read operations
{{% /alert %}}

## Example Configuration

Here's a complete example configuration for reading CSV files from a Windows SMB share:

```properties
# Connector configuration
name=smb-file-pulse-connector
connector.class=io.streamthoughts.kafka.connect.filepulse.source.FilePulseSourceConnector
fs.listing.class=io.streamthoughts.kafka.connect.filepulse.fs.SmbFileSystemListing

# SMB server configuration
smb.listing.host=192.168.1.100
smb.listing.port=445
smb.listing.share=shared_files
smb.listing.domain=MYDOMAIN
smb.listing.user=kafkauser
smb.listing.password=secretpassword
smb.listing.directory.path=/data/csv

# Connection and protocol configuration
smb.connection.timeout=30000
smb.response.timeout=30000
smb.so.timeout=30000
smb.connection.retries=5
smb.connection.retries.delay=5000
smb.protocol.max.version=SMB311

# File reader configuration
file.filter.regex.pattern=.*\\.csv$
tasks.reader.class=io.streamthoughts.kafka.connect.filepulse.fs.reader.SmbRowFileInputReader

# Kafka topic
kafka.topic=smb-data-topic
```

## Supported Features

- ✅ **SMB 3.0/3.0.2/3.1.1** protocol support only (SMB 1.0 and 2.x disabled)
- ✅ **Windows Server 2012+** and **Samba 4.x** compatibility
- ✅ **Domain authentication** (Active Directory)
- ✅ **NTLMv2 authentication** (automatically negotiated by jCIFS library)
- ✅ **SMB 3.x encryption** support for secure file transfers (automatically negotiated)
- ✅ **Automatic retry** on connection failures
- ✅ **File operations**: list, read, move, delete
- ✅ **Large file support** with streaming
- ✅ **Proper resource management** to prevent file lock issues

## Security Considerations

### Protocol Security

The connector enforces the following security measures:
- **SMB 3.0** minimum version (hardcoded for security)
- **SMB 3.1.1** maximum version (configurable, default)
- **SMB signing** is enabled by default for message integrity
- **SMB encryption** is automatically negotiated when supported by the server

### Network

- Ensure SMB port (445) is accessible from Kafka Connect workers

### Permissions

Grant the SMB user only the necessary permissions:
- **Read** permission for file listing and ingestion
- **Write** and **Delete** permissions only if using file move/cleanup strategies

## Troubleshooting

### Connection Issues

If you encounter connection problems:

1. **Verify network connectivity**: `ping <smb-host>`
2. **Check SMB port**: `telnet <smb-host> 445` or `nc -zv <smb-host> 445`
3. **Test credentials**: Try connecting manually with `smbclient` (Linux) or Windows Explorer
4. **Check firewall rules**: Ensure port 445 is not blocked
5. **Verify SMB 3.x support**: Check server configuration supports SMB 3.0 or later

Example smbclient test:
```bash
smbclient //<smb-host>/<share-name> -U <domain>/<username> -m SMB3
```

## Compatibility

### Supported Platforms

- **Windows Server**: 2012 and later (SMB 3.0 introduced)
- **Windows Client**: Windows 8/10/11
- **Samba**: 4.0 and later (SMB 3.0 support)
- **NAS devices**: Modern NAS with SMB 3.0+ support

### Unsupported (Security Reasons)

- ❌ **Windows Server 2008 R2** and earlier (SMB 2.1 and older)
- ❌ **Samba 3.x** (no SMB 3.0 support)
- ❌ **Legacy NAS** devices without SMB 3.0
- ❌ **SMB 1.0 clients/servers** (deprecated, security risk)

## Known Limitations

- Kerberos authentication is not supported (only NTLMv2)

## Internal Implementation Details

The connector uses the [jCIFS library (version 2.1.40)](https://github.com/codelibs/jcifs) for SMB protocol implementation. 

**Configurable jCIFS properties:**
- **Connection timeout**: Configurable via `smb.connection.timeout` (maps to `jcifs.smb.client.connTimeout`)
- **Response timeout**: Configurable via `smb.response.timeout` (maps to `jcifs.smb.client.responseTimeout`)
- **Socket timeout**: Configurable via `smb.so.timeout` (maps to `jcifs.smb.client.soTimeout`)
- **SMB protocol versions**: Min=SMB300 (hardcoded), Max=configurable (default SMB311)
