/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.fs;

import io.streamthoughts.kafka.connect.filepulse.internal.StringUtils;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;

/**
 * The aliyun OSS client's configuration.
 */
public class AliyunOSSClientConfig extends AbstractConfig {

    /**
     * OSS access key id
     */
    public static final String OSS_ACCESS_KEY_ID_CONFIG = "oss.access.key.id";
    /**
     * OSS secret key
     */
    public static final String OSS_SECRET_KEY_CONFIG = "oss.secret.key";
    /**
     * OSS access endpoint
     */
    public static final String OSS_ENDPOINT_CONFIG = "oss.endpoint";
    /**
     * OSS bucket name
     */
    public final static String OSS_BUCKET_NAME_CONFIG = "oss.bucket.name";
    /**
     * oss bucket prefix
     */
    public final static String OSS_BUCKET_PREFIX_CONFIG = "oss.bucket.prefix";
    /**
     * oss max connections
     */
    public static final String OSS_MAX_CONNECTIONS_CONFIG = "oss.max.connections";
    public static final int OSS_MAX_CONNECTIONS_DEFAULT = 1024;
    public static final String OSS_MAX_CONNECTIONS_DOC = "OSS max connections.";
    /**
     * oss socket timeout
     */
    public static final String OSS_SOCKET_TIMEOUT_CONFIG = "oss.socket.timeout";
    public static final int OSS_SOCKET_TIMEOUT_DEFAULT = 10 * 1000;
    public static final String OSS_SOCKET_TIMEOUT_DOC = "OSS connection timeout.";
    /**
     * oss connection timeout
     */
    public static final int OSS_CONNECTION_TIMEOUT = 50 * 1000;
    public static final String OSS_CONNECTION_TIMEOUT_CONFIG = "oss.connection.timeout";
    public static final String OSS_CONNECTION_TIMEOUT_DOC = "OSS connection timeout.";
    /**
     * oss max error retry
     */
    public static final String OSS_MAX_ERROR_RETRIES_CONFIG = "oss.max.error.retries";
    public static final int OSS_MAX_ERROR_RETRIES_DEFAULT = 5;
    public static final String OSS_MAX_ERROR_RETRIES_DOC =
            "The maximum number of retry attempts for failed retryable requests.";
    /**
     * OSS object storage class config
     */
    public static final String OSS_OBJECT_STORAGE_CLASS_CONFIG = "oss.default.object.storage.class";
    public static final String OSS_OBJECT_STORAGE_CLASS_DEFAULT = "Standard";
    public static final String OSS_OBJECT_STORAGE_CLASS_DOC =
            "The OSS storage class to associate with an OSS object when it is copied by the connector (e.g., during a move operation).";
    private static final String OSS_ACCESS_KEY_ID_DOC = "OSS Access Key ID";
    private static final String OSS_SECRET_ACCESS_KEY_DOC = "OSS Secret Access Key";
    private static final String OSS_ENDPOINT_DOC = "OSS access endpoint.";
    private final static String OSS_BUCKET_NAME_DOC = "The name of the Aliyun OSS bucket.";
    private final static String OSS_BUCKET_PREFIX_DOC =
            "The prefix to be used for restricting the listing of the objects in the bucket";
    private static final String OSS_GROUP_CONFIG = "OSS";

    /**
     * Creates a new {@link AliyunOSSClientConfig} instance.
     *
     * @param originals the original configuration map.
     */
    public AliyunOSSClientConfig(final Map<String, ?> originals) {
        super(getConf(), originals, false);
    }

    /**
     * @return the {@link ConfigDef}.
     */
    static ConfigDef getConf() {
        int ossGroupCounter = 0;

        return new ConfigDef().define(OSS_BUCKET_NAME_CONFIG, ConfigDef.Type.STRING,
                        ConfigDef.Importance.HIGH, OSS_BUCKET_NAME_DOC, OSS_GROUP_CONFIG, ossGroupCounter++,
                        ConfigDef.Width.NONE, OSS_BUCKET_NAME_CONFIG)

                .define(OSS_BUCKET_PREFIX_CONFIG, ConfigDef.Type.STRING, null, new ConfigDef.NonEmptyString(),
                        ConfigDef.Importance.MEDIUM, OSS_BUCKET_PREFIX_DOC, OSS_GROUP_CONFIG, ossGroupCounter++,
                        ConfigDef.Width.NONE, OSS_BUCKET_PREFIX_CONFIG)

                .define(OSS_ENDPOINT_CONFIG, ConfigDef.Type.STRING,
                        ConfigDef.Importance.MEDIUM, OSS_ENDPOINT_DOC, OSS_GROUP_CONFIG, ossGroupCounter++,
                        ConfigDef.Width.NONE, OSS_ENDPOINT_CONFIG)
                .define(OSS_ACCESS_KEY_ID_CONFIG, ConfigDef.Type.PASSWORD,
                        ConfigDef.Importance.HIGH, OSS_ACCESS_KEY_ID_DOC, OSS_GROUP_CONFIG, ossGroupCounter++,
                        ConfigDef.Width.NONE, OSS_ACCESS_KEY_ID_CONFIG)

                .define(OSS_SECRET_KEY_CONFIG, ConfigDef.Type.PASSWORD,
                        ConfigDef.Importance.HIGH, OSS_SECRET_ACCESS_KEY_DOC, OSS_GROUP_CONFIG, ossGroupCounter++,
                        ConfigDef.Width.NONE, OSS_SECRET_KEY_CONFIG)

                .define(OSS_MAX_ERROR_RETRIES_CONFIG, ConfigDef.Type.INT, OSS_MAX_ERROR_RETRIES_DEFAULT,
                        ConfigDef.Range.atLeast(1), ConfigDef.Importance.MEDIUM, OSS_MAX_ERROR_RETRIES_DOC,
                        OSS_GROUP_CONFIG, ossGroupCounter++, ConfigDef.Width.NONE, OSS_MAX_ERROR_RETRIES_CONFIG)
                .define(OSS_CONNECTION_TIMEOUT_CONFIG, ConfigDef.Type.INT, OSS_CONNECTION_TIMEOUT,
                        ConfigDef.Range.atLeast(1), ConfigDef.Importance.MEDIUM, OSS_CONNECTION_TIMEOUT_DOC,
                        OSS_GROUP_CONFIG, ossGroupCounter++, ConfigDef.Width.NONE, OSS_CONNECTION_TIMEOUT_CONFIG)

                .define(OSS_MAX_CONNECTIONS_CONFIG, ConfigDef.Type.INT, OSS_MAX_CONNECTIONS_DEFAULT,
                        ConfigDef.Range.atLeast(1), ConfigDef.Importance.MEDIUM, OSS_MAX_CONNECTIONS_DOC,
                        OSS_GROUP_CONFIG, ossGroupCounter++, ConfigDef.Width.NONE, OSS_MAX_CONNECTIONS_CONFIG)
                .define(OSS_SOCKET_TIMEOUT_CONFIG, ConfigDef.Type.INT, OSS_SOCKET_TIMEOUT_DEFAULT,
                        ConfigDef.Range.atLeast(1), ConfigDef.Importance.MEDIUM, OSS_SOCKET_TIMEOUT_DOC,
                        OSS_GROUP_CONFIG, ossGroupCounter++, ConfigDef.Width.NONE, OSS_SOCKET_TIMEOUT_CONFIG)
                .define(OSS_OBJECT_STORAGE_CLASS_CONFIG, ConfigDef.Type.STRING, OSS_OBJECT_STORAGE_CLASS_DEFAULT,
                        ConfigDef.Importance.LOW, OSS_OBJECT_STORAGE_CLASS_DOC, OSS_GROUP_CONFIG, ossGroupCounter++,
                        ConfigDef.Width.NONE, OSS_OBJECT_STORAGE_CLASS_CONFIG);
    }

    public Password getOSSAccessKeyId() {
        return getPassword(OSS_ACCESS_KEY_ID_CONFIG);
    }

    public Password getOSSAccessKey() {
        return getPassword(OSS_SECRET_KEY_CONFIG);
    }

    public String getOSSBucketName() {
        return getString(OSS_BUCKET_NAME_CONFIG);
    }

    public String getOSSEndpoint() {
        return getString(OSS_ENDPOINT_CONFIG);
    }

    public String getOSSBucketPrefix() {
        return getString(OSS_BUCKET_PREFIX_CONFIG);
    }

    public int getOSSMaxConnections() {
        return getInt(OSS_MAX_CONNECTIONS_CONFIG);
    }

    public int getOSSSocketTimeout() {
        return getInt(OSS_SOCKET_TIMEOUT_CONFIG);
    }

    public int getOSSConnectionTimeout() {
        return getInt(OSS_CONNECTION_TIMEOUT_CONFIG);
    }

    public int getOssMaxErrorRetries() {
        return getInt(OSS_MAX_ERROR_RETRIES_CONFIG);
    }

    public String getOSSDefaultStorageClass() {
        return getString(OSS_OBJECT_STORAGE_CLASS_CONFIG);
    }

    public static class NonEmptyPassword implements ConfigDef.Validator {
        @Override
        public void ensureValid(final String name, final Object value) {
            if (Objects.isNull(value) || ((Password) value).value() == null) {
                return;
            }
            final Password pwd = (Password) value;
            if (StringUtils.isBlank(pwd.value())) {
                throw new ConfigException(name, pwd, "Password must be non-empty");
            }
        }
    }

}
