/*
 * Copyright 2019-2023 StreamThoughts.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.streamthoughts.kafka.connect.filepulse.fs;

import io.streamthoughts.kafka.connect.filepulse.errors.ConnectFilePulseException;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SftpFilesystemListingConfig extends AbstractConfig {
    private static final Logger log = LoggerFactory.getLogger(SftpFilesystemListingConfig.class);

    public static final String CONNECTOR_NAME_PROPERTY = "name";

    public static final String SFTP_LISTING_HOST = "sftp.listing.host";
    public static final String SFTP_LISTING_HOST_DOC = "Hostname to connect to";

    public static final String SFTP_LISTING_PORT = "sftp.listing.port";
    public static final String SFTP_LISTING_PORT_DOC = "Port to connect to";
    public static final Integer SFTP_LISTING_PORT_DEFAULT = 22;

    public static final String SFTP_LISTING_USER = "sftp.listing.user";
    public static final String SFTP_LISTING_USER_DOC = "SFTP username";

    public static final String SFTP_LISTING_PASSWORD = "sftp.listing.password";
    public static final String SFTP_LISTING_PASSWORD_DOC = "SFTP user credentials";

    public static final String SFTP_LISTING_DIRECTORY_PATH = "sftp.listing.directory.path";
    public static final String SFTP_LISTING_DIRECTORY_DOC = "The input directory to scan";

    public static final String SFTP_LISTING_STRICT_HOST_KEY_CHECK = "sftp.listing.strict.host.key.check";
    public static final String SFTP_LISTING_STRICT_HOST_KEY_CHECK_DOC = "String host key checking";
    public static final String SFTP_LISTING_STRICT_HOST_KEY_CHECK_DEFAULT = "no";

    public static final String SFTP_CONNECTION_TIMEOUT_MS = "sftp.connection.timeout";
    public static final String SFTP_CONNECTION_TIMEOUT_MS_DOC = "SFTP connection timeout in millis";
    public static final Integer SFTP_CONNECTION_TIMEOUT_MS_DEFAULT = 10000;

    public static final String SFTP_CONNECTION_RETRIES = "sftp.connection.retries";
    public static final String SFTP_CONNECTION_RETRIES_DOC = "SFTP connection retries";
    public static final Integer SFTP_CONNECTION_RETRIES_DEFAULT = 5;

    public static final String SFTP_CONNECTION_DELAY_MS = "sftp.connection.retries.delay";
    public static final String SFTP_CONNECTION_DELAY_DOC = "SFTP connection delay between retries in millis";
    public static final Integer SFTP_CONNECTION_DELAY_DEFAULT = 5000;

    public static ConfigDef getConf() {
        return new ConfigDef()
                .define(
                        SFTP_LISTING_DIRECTORY_PATH,
                        ConfigDef.Type.STRING,
                        ConfigDef.Importance.HIGH,
                        SFTP_LISTING_DIRECTORY_DOC
                )
                .define(
                        SFTP_LISTING_HOST,
                        ConfigDef.Type.STRING,
                        ConfigDef.Importance.HIGH,
                        SFTP_LISTING_HOST_DOC
                )
                .define(
                        SFTP_LISTING_USER,
                        ConfigDef.Type.STRING,
                        ConfigDef.Importance.HIGH,
                        SFTP_LISTING_USER_DOC
                )
                .define(
                        SFTP_LISTING_PASSWORD,
                        ConfigDef.Type.STRING,
                        ConfigDef.Importance.HIGH,
                        SFTP_LISTING_PASSWORD_DOC
                )
                .define(
                        SFTP_LISTING_PORT,
                        ConfigDef.Type.INT,
                        SFTP_LISTING_PORT_DEFAULT,
                        ConfigDef.Importance.HIGH,
                        SFTP_LISTING_PORT_DOC
                )
                .define(
                        SFTP_LISTING_STRICT_HOST_KEY_CHECK,
                        ConfigDef.Type.STRING,
                        SFTP_LISTING_STRICT_HOST_KEY_CHECK_DEFAULT,
                        ConfigDef.Importance.MEDIUM,
                        SFTP_LISTING_STRICT_HOST_KEY_CHECK_DOC
                )
                .define(
                        SFTP_CONNECTION_TIMEOUT_MS,
                        ConfigDef.Type.INT,
                        SFTP_CONNECTION_TIMEOUT_MS_DEFAULT,
                        ConfigDef.Importance.LOW,
                        SFTP_CONNECTION_TIMEOUT_MS_DOC
                )
                .define(
                        SFTP_CONNECTION_RETRIES,
                        ConfigDef.Type.INT,
                        SFTP_CONNECTION_RETRIES_DEFAULT,
                        ConfigDef.Importance.LOW,
                        SFTP_CONNECTION_RETRIES_DOC
                )
                .define(
                        SFTP_CONNECTION_DELAY_MS,
                        ConfigDef.Type.INT,
                        SFTP_CONNECTION_DELAY_DEFAULT,
                        ConfigDef.Importance.LOW,
                        SFTP_CONNECTION_DELAY_DOC
                );
    }

    public SftpFilesystemListingConfig(Map<?, ?> originals) {
        super(getConf(), originals, true);
    }

    public String getSftpListingHost() {
        return getString(SFTP_LISTING_HOST);
    }

    public Integer getSftpListingPort() {
        return getInt(SFTP_LISTING_PORT);
    }

    public String getSftpListingDirectoryPath() {
        String path = getString(SFTP_LISTING_DIRECTORY_PATH);
        return stripSlashEndingIfPresent(path);
    }

    private String stripSlashEndingIfPresent(String path) {
        return path.endsWith("/") ? path.substring(0, path.length() - 1) : path;
    }

    public String getSftpListingUser() {
        return getString(SFTP_LISTING_USER);
    }

    public String getSftpListingPassword() {
        return getString(SFTP_LISTING_PASSWORD);
    }

    public String getSftpListingStrictHostKeyCheck() {
        return getString(SFTP_LISTING_STRICT_HOST_KEY_CHECK);
    }

    public Integer getSftpConnectionTimeoutMs() {
        return getInt(SFTP_CONNECTION_TIMEOUT_MS);
    }

    public String getConnectorName() {
        String connectorName = Optional.ofNullable(originalsStrings().get(CONNECTOR_NAME_PROPERTY))
                .orElseThrow(() -> new ConnectFilePulseException("Cannot find property 'name' in config"));
        log.debug("Connector name: {}", connectorName);

        return connectorName;
    }

    public Integer getSftpRetries() {
        return getInt(SFTP_CONNECTION_RETRIES);
    }

    public Integer getSftpDelayBetweenRetriesMs() {
        return getInt(SFTP_CONNECTION_DELAY_MS);
    }
}