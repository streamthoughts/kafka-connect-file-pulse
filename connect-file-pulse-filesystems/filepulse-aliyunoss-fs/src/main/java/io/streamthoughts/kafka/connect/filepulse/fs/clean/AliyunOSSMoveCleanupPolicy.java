/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.fs.clean;

import static io.streamthoughts.kafka.connect.filepulse.fs.clean.AliyunOSSMoveCleanupPolicy.Config.FAILURES_OSS_BUCKET_NAME_CONFIG;
import static io.streamthoughts.kafka.connect.filepulse.fs.clean.AliyunOSSMoveCleanupPolicy.Config.FAILURES_OSS_PREFIX_PATH_CONFIG;
import static io.streamthoughts.kafka.connect.filepulse.fs.clean.AliyunOSSMoveCleanupPolicy.Config.SUCCESS_OSS_BUCKET_NAME_CONFIG;
import static io.streamthoughts.kafka.connect.filepulse.fs.clean.AliyunOSSMoveCleanupPolicy.Config.SUCCESS_OSS_PREFIX_PATH_CONFIG;

import io.streamthoughts.kafka.connect.filepulse.clean.FileCleanupPolicy;
import io.streamthoughts.kafka.connect.filepulse.fs.AliyunOSSStorage;
import io.streamthoughts.kafka.connect.filepulse.fs.OSSBucketKey;
import io.streamthoughts.kafka.connect.filepulse.fs.Storage;
import io.streamthoughts.kafka.connect.filepulse.source.FileObject;
import java.net.URI;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AliyunOSSMoveCleanupPolicy implements FileCleanupPolicy {

    private static final Logger LOG = LoggerFactory.getLogger(AliyunOSSMoveCleanupPolicy.class);
    private AliyunOSSStorage storage;
    private Config config;

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(final Map<String, ?> configs) {
        this.config = new Config(configs);
    }

    @Override
    public boolean onSuccess(final FileObject source) {
        return move(source, SUCCESS_OSS_BUCKET_NAME_CONFIG, SUCCESS_OSS_PREFIX_PATH_CONFIG);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean onFailure(final FileObject source) {
        return move(source, FAILURES_OSS_BUCKET_NAME_CONFIG, FAILURES_OSS_PREFIX_PATH_CONFIG);
    }

    private boolean move(final FileObject source, final String destinationOSSBucketConfig,
                         final String destinationOSSPrefixConfig) {
        checkState();
        URI sourceURI = source.metadata().uri();
        if (!storage.exists(sourceURI)) {
            LOG.warn("Cannot move object-file '{}' to failure OSS bucket due to file does not exist.", sourceURI);
            return true;
        }
        OSSBucketKey sourceBucketKey = OSSBucketKey.fromURI(sourceURI);

        String destOSSBucketName =
                Optional.ofNullable(config.getString(destinationOSSBucketConfig)).orElse(sourceBucketKey.bucketName());

        OSSBucketKey destBucketKey = new OSSBucketKey(destOSSBucketName, config.getString(destinationOSSPrefixConfig),
                sourceBucketKey.objectName());
        return storage.move(sourceURI, destBucketKey.toURI());
    }

    @Override
    public void setStorage(final Storage storage) {
        this.storage = (AliyunOSSStorage) storage;
    }

    private void checkState() {
        if (storage == null) {
            throw new IllegalStateException("no 'storage' initialized.");
        }
    }

    public static class Config extends AbstractConfig {
        private static final String CONFIG_GROUP = "AliyunOSSMoveCleanupPolicy";

        private static final String CONFIG_PREFIX = "fs.cleanup.policy.move.";

        /**
         * success oss bucket name config
         */
        public static final String SUCCESS_OSS_BUCKET_NAME_CONFIG = CONFIG_PREFIX + "success.oss.bucket.name";
        /**
         * success oss prefix path config
         */
        public static final String SUCCESS_OSS_PREFIX_PATH_CONFIG = CONFIG_PREFIX + "success.oss.prefix.path";
        /**
         * failure oss bucket name config
         */
        public static final String FAILURES_OSS_BUCKET_NAME_CONFIG = CONFIG_PREFIX + "failure.oss.bucket.name";
        /**
         * failure oss prefix path config
         */
        public static final String FAILURES_OSS_PREFIX_PATH_CONFIG = CONFIG_PREFIX + "failure.oss.prefix.path";
        private static final String SUCCESS_OSS_BUCKET_NAME_DOC =
                "The name of the destination OSS bucket for success objects.";
        private static final String SUCCESS_OSS_PREFIX_PATH_DOC =
                "The prefix to be used for defining the key of an OSS object to move into the destination bucket.";
        private static final String FAILURES_OSS_BUCKET_NAME_DOC =
                "The name of the destination OSS bucket for failure objects.";
        private static final String FAILURES_OSS_PREFIX_PATH_DOC =
                "The prefix to be used for defining the key of OSS object to move into the destination bucket.";

        /**
         * Creates a new {@link Config} instance.
         */
        public Config(final Map<?, ?> originals) {
            super(configDef(), originals, true);
        }

        static ConfigDef configDef() {
            int groupCounter = 0;
            return new ConfigDef().define(SUCCESS_OSS_BUCKET_NAME_CONFIG, ConfigDef.Type.STRING, null,
                            ConfigDef.Importance.HIGH, SUCCESS_OSS_BUCKET_NAME_DOC, CONFIG_GROUP, groupCounter++,
                            ConfigDef.Width.NONE, SUCCESS_OSS_BUCKET_NAME_CONFIG)
                    .define(SUCCESS_OSS_PREFIX_PATH_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
                            ConfigDef.Importance.HIGH, SUCCESS_OSS_PREFIX_PATH_DOC, CONFIG_GROUP, groupCounter++,
                            ConfigDef.Width.NONE, SUCCESS_OSS_PREFIX_PATH_CONFIG)
                    .define(FAILURES_OSS_PREFIX_PATH_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
                            ConfigDef.Importance.HIGH, FAILURES_OSS_PREFIX_PATH_DOC, CONFIG_GROUP, groupCounter++,
                            ConfigDef.Width.NONE, FAILURES_OSS_PREFIX_PATH_CONFIG)
                    .define(FAILURES_OSS_BUCKET_NAME_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH,
                            FAILURES_OSS_BUCKET_NAME_DOC, CONFIG_GROUP, groupCounter++, ConfigDef.Width.NONE,
                            FAILURES_OSS_BUCKET_NAME_CONFIG);
        }
    }
}
