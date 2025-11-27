/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.fs;

import java.util.Map;
import jcifs.SmbConstants;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

/**
 * Configuration for SMB filesystem listing.
 */
public class SmbFileSystemListingConfig extends AbstractConfig {

    public static final String SMB_LISTING_HOST_CONFIG = "smb.listing.host";
    private static final String SMB_LISTING_HOST_DOC = "SMB server hostname or IP address";

    public static final String SMB_LISTING_PORT_CONFIG = "smb.listing.port";
    private static final String SMB_LISTING_PORT_DOC = "SMB server port";
    private static final int SMB_LISTING_PORT_DEFAULT = 445;

    public static final String SMB_LISTING_SHARE_CONFIG = "smb.listing.share";
    private static final String SMB_LISTING_SHARE_DOC = "SMB share name";

    public static final String SMB_LISTING_DOMAIN_CONFIG = "smb.listing.domain";
    private static final String SMB_LISTING_DOMAIN_DOC = "SMB domain/workgroup";
    private static final String SMB_LISTING_DOMAIN_DEFAULT = "WORKGROUP";

    public static final String SMB_LISTING_USER_CONFIG = "smb.listing.user";
    private static final String SMB_LISTING_USER_DOC = "SMB username";

    public static final String SMB_LISTING_PASSWORD_CONFIG = "smb.listing.password";
    private static final String SMB_LISTING_PASSWORD_DOC = "SMB password";

    public static final String SMB_LISTING_DIRECTORY_PATH_CONFIG = "smb.listing.directory.path";
    private static final String SMB_LISTING_DIRECTORY_PATH_DOC = "The directory path on the SMB share to scan";
    private static final String SMB_LISTING_DIRECTORY_PATH_DEFAULT = "/";

    public static final String SMB_CONNECTION_TIMEOUT_CONFIG = "smb.connection.timeout";
    private static final String SMB_CONNECTION_TIMEOUT_DOC = "SMB connection timeout in milliseconds (jcifs.smb.client.connTimeout)";
    private static final int SMB_CONNECTION_TIMEOUT_DEFAULT = SmbConstants.DEFAULT_CONN_TIMEOUT;

    public static final String SMB_RESPONSE_TIMEOUT_CONFIG = "smb.response.timeout";
    private static final String SMB_RESPONSE_TIMEOUT_DOC = "SMB response timeout in milliseconds (jcifs.smb.client.responseTimeout)";
    private static final int SMB_RESPONSE_TIMEOUT_DEFAULT = SmbConstants.DEFAULT_RESPONSE_TIMEOUT;

    public static final String SMB_SO_TIMEOUT_CONFIG = "smb.so.timeout";
    private static final String SMB_SO_TIMEOUT_DOC = "SMB socket timeout in milliseconds (jcifs.smb.client.soTimeout)";
    private static final int SMB_SO_TIMEOUT_DEFAULT = SmbConstants.DEFAULT_SO_TIMEOUT;

    public static final String SMB_CONNECTION_RETRIES_CONFIG = "smb.connection.retries";
    private static final String SMB_CONNECTION_RETRIES_DOC = "Number of retries for SMB connection failures";
    private static final int SMB_CONNECTION_RETRIES_DEFAULT = 5;

    public static final String SMB_CONNECTION_RETRIES_DELAY_CONFIG = "smb.connection.retries.delay";
    private static final String SMB_CONNECTION_RETRIES_DELAY_DOC = "Delay between connection retries in milliseconds";
    private static final int SMB_CONNECTION_RETRIES_DELAY_DEFAULT = 5000;

    public static final String SMB_MAX_VERSION_CONFIG = "smb.protocol.max.version";
    private static final String SMB_MAX_VERSION_DOC = "Maximum SMB protocol version to use (SMB300, SMB302, SMB311)";
    private static final String SMB_MAX_VERSION_DEFAULT = "SMB311";

    public SmbFileSystemListingConfig(Map<String, ?> originals) {
        super(configDef(), originals);
    }

    public String getSmbHost() {
        return getString(SMB_LISTING_HOST_CONFIG);
    }

    public int getSmbPort() {
        return getInt(SMB_LISTING_PORT_CONFIG);
    }

    public String getSmbShare() {
        return getString(SMB_LISTING_SHARE_CONFIG);
    }

    public String getSmbDomain() {
        return getString(SMB_LISTING_DOMAIN_CONFIG);
    }

    public String getSmbUser() {
        return getString(SMB_LISTING_USER_CONFIG);
    }

    public String getSmbPassword() {
        return getPassword(SMB_LISTING_PASSWORD_CONFIG).value();
    }

    public String getSmbDirectoryPath() {
        return getString(SMB_LISTING_DIRECTORY_PATH_CONFIG);
    }

    public int getConnectionTimeout() {
        return getInt(SMB_CONNECTION_TIMEOUT_CONFIG);
    }

    public int getResponseTimeout() {
        return getInt(SMB_RESPONSE_TIMEOUT_CONFIG);
    }

    public int getSoTimeout() {
        return getInt(SMB_SO_TIMEOUT_CONFIG);
    }

    public int getConnectionRetries() {
        return getInt(SMB_CONNECTION_RETRIES_CONFIG);
    }

    public int getConnectionRetriesDelay() {
        return getInt(SMB_CONNECTION_RETRIES_DELAY_CONFIG);
    }

    public String getSmbMaxVersion() {
        return getString(SMB_MAX_VERSION_CONFIG);
    }

    public static ConfigDef configDef() {
        int configGroupCounter = 0;
        return new ConfigDef()
                .define(
                        SMB_LISTING_HOST_CONFIG,
                        ConfigDef.Type.STRING,
                        ConfigDef.NO_DEFAULT_VALUE,
                        ConfigDef.Importance.HIGH,
                        SMB_LISTING_HOST_DOC,
                        "SMB",
                        configGroupCounter++,
                        ConfigDef.Width.NONE,
                        SMB_LISTING_HOST_CONFIG
                )
                .define(
                        SMB_LISTING_PORT_CONFIG,
                        ConfigDef.Type.INT,
                        SMB_LISTING_PORT_DEFAULT,
                        ConfigDef.Importance.HIGH,
                        SMB_LISTING_PORT_DOC,
                        "SMB",
                        configGroupCounter++,
                        ConfigDef.Width.NONE,
                        SMB_LISTING_PORT_CONFIG
                )
                .define(
                        SMB_LISTING_SHARE_CONFIG,
                        ConfigDef.Type.STRING,
                        ConfigDef.NO_DEFAULT_VALUE,
                        ConfigDef.Importance.HIGH,
                        SMB_LISTING_SHARE_DOC,
                        "SMB",
                        configGroupCounter++,
                        ConfigDef.Width.NONE,
                        SMB_LISTING_SHARE_CONFIG
                )
                .define(
                        SMB_LISTING_DOMAIN_CONFIG,
                        ConfigDef.Type.STRING,
                        SMB_LISTING_DOMAIN_DEFAULT,
                        ConfigDef.Importance.MEDIUM,
                        SMB_LISTING_DOMAIN_DOC,
                        "SMB",
                        configGroupCounter++,
                        ConfigDef.Width.NONE,
                        SMB_LISTING_DOMAIN_CONFIG
                )
                .define(
                        SMB_LISTING_USER_CONFIG,
                        ConfigDef.Type.STRING,
                        ConfigDef.NO_DEFAULT_VALUE,
                        ConfigDef.Importance.HIGH,
                        SMB_LISTING_USER_DOC,
                        "SMB",
                        configGroupCounter++,
                        ConfigDef.Width.NONE,
                        SMB_LISTING_USER_CONFIG
                )
                .define(
                        SMB_LISTING_PASSWORD_CONFIG,
                        ConfigDef.Type.PASSWORD,
                        ConfigDef.NO_DEFAULT_VALUE,
                        ConfigDef.Importance.HIGH,
                        SMB_LISTING_PASSWORD_DOC,
                        "SMB",
                        configGroupCounter++,
                        ConfigDef.Width.NONE,
                        SMB_LISTING_PASSWORD_CONFIG
                )
                .define(
                        SMB_LISTING_DIRECTORY_PATH_CONFIG,
                        ConfigDef.Type.STRING,
                        SMB_LISTING_DIRECTORY_PATH_DEFAULT,
                        ConfigDef.Importance.HIGH,
                        SMB_LISTING_DIRECTORY_PATH_DOC,
                        "SMB",
                        configGroupCounter++,
                        ConfigDef.Width.NONE,
                        SMB_LISTING_DIRECTORY_PATH_CONFIG
                )
                .define(
                        SMB_CONNECTION_TIMEOUT_CONFIG,
                        ConfigDef.Type.INT,
                        SMB_CONNECTION_TIMEOUT_DEFAULT,
                        ConfigDef.Importance.MEDIUM,
                        SMB_CONNECTION_TIMEOUT_DOC,
                        "SMB",
                        configGroupCounter++,
                        ConfigDef.Width.NONE,
                        SMB_CONNECTION_TIMEOUT_CONFIG
                )
                .define(
                        SMB_RESPONSE_TIMEOUT_CONFIG,
                        ConfigDef.Type.INT,
                        SMB_RESPONSE_TIMEOUT_DEFAULT,
                        ConfigDef.Importance.MEDIUM,
                        SMB_RESPONSE_TIMEOUT_DOC,
                        "SMB",
                        configGroupCounter++,
                        ConfigDef.Width.NONE,
                        SMB_RESPONSE_TIMEOUT_CONFIG
                )
                .define(
                        SMB_SO_TIMEOUT_CONFIG,
                        ConfigDef.Type.INT,
                        SMB_SO_TIMEOUT_DEFAULT,
                        ConfigDef.Importance.MEDIUM,
                        SMB_SO_TIMEOUT_DOC,
                        "SMB",
                        configGroupCounter++,
                        ConfigDef.Width.NONE,
                        SMB_SO_TIMEOUT_CONFIG
                )
                .define(
                        SMB_CONNECTION_RETRIES_CONFIG,
                        ConfigDef.Type.INT,
                        SMB_CONNECTION_RETRIES_DEFAULT,
                        ConfigDef.Importance.MEDIUM,
                        SMB_CONNECTION_RETRIES_DOC,
                        "SMB",
                        configGroupCounter++,
                        ConfigDef.Width.NONE,
                        SMB_CONNECTION_RETRIES_CONFIG
                )
                .define(
                        SMB_CONNECTION_RETRIES_DELAY_CONFIG,
                        ConfigDef.Type.INT,
                        SMB_CONNECTION_RETRIES_DELAY_DEFAULT,
                        ConfigDef.Importance.MEDIUM,
                        SMB_CONNECTION_RETRIES_DELAY_DOC,
                        "SMB",
                        configGroupCounter++,
                        ConfigDef.Width.NONE,
                        SMB_CONNECTION_RETRIES_DELAY_CONFIG
                )
                .define(
                        SMB_MAX_VERSION_CONFIG,
                        ConfigDef.Type.STRING,
                        SMB_MAX_VERSION_DEFAULT,
                        ConfigDef.Importance.MEDIUM,
                        SMB_MAX_VERSION_DOC,
                        "SMB",
                        configGroupCounter++,
                        ConfigDef.Width.NONE,
                        SMB_MAX_VERSION_CONFIG
                );
    }
}
