/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.fs.clean;

import static io.streamthoughts.kafka.connect.filepulse.fs.clean.AmazonS3MoveCleanupPolicy.Config.EXCLUDE_SOURCE_PREFIX_PATH_CONFIG;
import static io.streamthoughts.kafka.connect.filepulse.fs.clean.AmazonS3MoveCleanupPolicy.Config.FAILURES_AWS_BUCKET_NAME_CONFIG;
import static io.streamthoughts.kafka.connect.filepulse.fs.clean.AmazonS3MoveCleanupPolicy.Config.FAILURES_AWS_INCLUDE_SOURCE_PREFIX_PATH;
import static io.streamthoughts.kafka.connect.filepulse.fs.clean.AmazonS3MoveCleanupPolicy.Config.FAILURES_AWS_PREFIX_PATH_CONFIG;
import static io.streamthoughts.kafka.connect.filepulse.fs.clean.AmazonS3MoveCleanupPolicy.Config.SUCCESS_AWS_BUCKET_NAME_CONFIG;
import static io.streamthoughts.kafka.connect.filepulse.fs.clean.AmazonS3MoveCleanupPolicy.Config.SUCCESS_AWS_INCLUDE_SOURCE_PREFIX_PATH;
import static io.streamthoughts.kafka.connect.filepulse.fs.clean.AmazonS3MoveCleanupPolicy.Config.SUCCESS_AWS_PREFIX_PATH_CONFIG;

import io.streamthoughts.kafka.connect.filepulse.clean.FileCleanupPolicy;
import io.streamthoughts.kafka.connect.filepulse.fs.AmazonS3Storage;
import io.streamthoughts.kafka.connect.filepulse.fs.S3BucketKey;
import io.streamthoughts.kafka.connect.filepulse.fs.Storage;
import io.streamthoughts.kafka.connect.filepulse.source.FileObject;
import java.net.URI;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AmazonS3MoveCleanupPolicy implements FileCleanupPolicy {

    private static final Logger LOG = LoggerFactory.getLogger(AmazonS3MoveCleanupPolicy.class);

    private AmazonS3Storage storage;
    
    private boolean includeSuccessSourcePrefixPath;
    private boolean includeFailuresSourcePrefixPath;
    
    private Config config;

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(final Map<String, ?> configs) {
        this.config = new Config(configs);
        this.includeSuccessSourcePrefixPath = this.config.getBoolean(SUCCESS_AWS_INCLUDE_SOURCE_PREFIX_PATH);
        this.includeFailuresSourcePrefixPath = this.config.getBoolean(FAILURES_AWS_INCLUDE_SOURCE_PREFIX_PATH);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean onSuccess(final FileObject source) {
        return move(
                source, 
                SUCCESS_AWS_BUCKET_NAME_CONFIG, 
                SUCCESS_AWS_PREFIX_PATH_CONFIG,
                includeSuccessSourcePrefixPath);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean onFailure(final FileObject source) {
        return move(
                source, 
                FAILURES_AWS_BUCKET_NAME_CONFIG, 
                FAILURES_AWS_PREFIX_PATH_CONFIG,
                includeFailuresSourcePrefixPath);
    }

    private boolean move(final FileObject source,
                         final String destinationS3BucketConfig,
                         final String destinationS3PrefixConfig,
                         final boolean includeSourcePrefixPath) {
        checkState();
        URI sourceURI = source.metadata().uri();
        if (!storage.exists(sourceURI)) {
            LOG.warn("Cannot move object-file '{}' to failure S3 bucket due to file does not exist.", sourceURI);
            return true;
        }
        S3BucketKey sourceBucketKey = S3BucketKey.fromURI(sourceURI);

        String relativeSourcePrefix = extractPrefix(
                sourceBucketKey.key().replaceAll(sourceBucketKey.objectName(), ""));
        String newObjectKey = includeSourcePrefixPath ? 
                relativeSourcePrefix + sourceBucketKey.objectName() : sourceBucketKey.objectName();

        var destS3BucketName = Optional
                .ofNullable(config.getString(destinationS3BucketConfig))
                .orElse(sourceBucketKey.bucketName());

        var destBucketKey = new S3BucketKey(
                destS3BucketName,
                config.getString(destinationS3PrefixConfig),
                newObjectKey
        );
        return storage.move(sourceURI, destBucketKey.toURI());
    }

    private String extractPrefix(final String p) {
        String excludeSourcePrefixPath = Optional
                .ofNullable(config.getString(EXCLUDE_SOURCE_PREFIX_PATH_CONFIG))
                .orElse("");
        String prefix = p.replaceAll(excludeSourcePrefixPath, "");
        prefix = prefix.replaceAll("^/+", "");
        // if there are no subdirectories, return an empty string
        if (prefix.length() == 0) {
            return "";
        }
        return prefix.endsWith("/") ? prefix : prefix + "/";
    }
    /**
     * {@inheritDoc}
     */
    @Override
    public void setStorage(final Storage storage) {
        this.storage = (AmazonS3Storage)storage;
    }

    private void checkState() {
        if (storage == null) {
            throw new IllegalStateException("no 'storage' initialized.");
        }
    }


    public static class Config extends AbstractConfig {

        private static final String CONFIG_GROUP = "AmazonS3MoveCleanupPolicy";

        private static final String CONFIG_PREFIX = "fs.cleanup.policy.move.";

        public static final String SUCCESS_AWS_BUCKET_NAME_CONFIG =
                CONFIG_PREFIX + "success.aws.bucket.name";
        private static final String SUCCESS_AWS_BUCKET_NAME_DOC =
                "The name of the destination S3 bucket for success objects.";

        public static final String SUCCESS_AWS_PREFIX_PATH_CONFIG =
                CONFIG_PREFIX + "success.aws.prefix.path";
        private static final String SUCCESS_AWS_PREFIX_PATH_DOC =
                "The prefix to be used for defining the key of an S3 object to move into the destination bucket.";

        public static final String SUCCESS_AWS_INCLUDE_SOURCE_PREFIX_PATH = 
                CONFIG_PREFIX + "success.aws.include.source.prefix.path";
        private static final String SUCCESS_AWS_INCLUDE_SOURCE_PREFIX_PATH_DOC =
                "Indicates whether to include the source prefix path in the destination key.";
        public static final String FAILURES_AWS_INCLUDE_SOURCE_PREFIX_PATH = 
                CONFIG_PREFIX + "failure.aws.include.source.prefix.path";
        private static final String FAILURES_AWS_INCLUDE_SOURCE_PREFIX_PATH_DOC =
                "Indicates whether to include the source prefix path in the destination key.";
        public static final String FAILURES_AWS_BUCKET_NAME_CONFIG =
                CONFIG_PREFIX + "failure.aws.bucket.name";
        private static final String FAILURES_AWS_BUCKET_NAME_DOC =
                "The name of the destination S3 bucket for failure objects.";

        public static final String FAILURES_AWS_PREFIX_PATH_CONFIG =
                CONFIG_PREFIX + "failure.aws.prefix.path";
        private static final String FAILURES_AWS_PREFIX_PATH_DOC =
                "The prefix to be used for defining the key of S3 object to move into the destination bucket.";

        public static final String EXCLUDE_SOURCE_PREFIX_PATH_CONFIG = 
                CONFIG_PREFIX + "exclude.source.prefix.path";
        private static final String EXCLUDE_SOURCE_PREFIX_PATH_DOC =
                "Indicates whether to exclude the source prefix path from the destination key.";

        /**
         * Creates a new {@link Config} instance.
         */
        public Config(final Map<?, ?> originals) {
            super(configDef(), originals, true);
        }

        static ConfigDef configDef() {
            int groupCounter = 0;
            return new ConfigDef()
                    .define(
                            SUCCESS_AWS_BUCKET_NAME_CONFIG,
                            ConfigDef.Type.STRING,
                            null,
                            ConfigDef.Importance.HIGH,
                            SUCCESS_AWS_BUCKET_NAME_DOC,
                            CONFIG_GROUP,
                            groupCounter++,
                            ConfigDef.Width.NONE,
                            SUCCESS_AWS_BUCKET_NAME_CONFIG
                    )
                    .define(
                            SUCCESS_AWS_PREFIX_PATH_CONFIG,
                            ConfigDef.Type.STRING,
                            ConfigDef.NO_DEFAULT_VALUE,
                            ConfigDef.Importance.HIGH,
                            SUCCESS_AWS_PREFIX_PATH_DOC,
                            CONFIG_GROUP,
                            groupCounter++,
                            ConfigDef.Width.NONE,
                            SUCCESS_AWS_PREFIX_PATH_CONFIG
                    )
                    .define(
                            SUCCESS_AWS_INCLUDE_SOURCE_PREFIX_PATH,
                            ConfigDef.Type.BOOLEAN,
                            false,
                            ConfigDef.Importance.LOW,
                            SUCCESS_AWS_INCLUDE_SOURCE_PREFIX_PATH_DOC,
                            CONFIG_GROUP,
                            groupCounter++,
                            ConfigDef.Width.NONE,
                            SUCCESS_AWS_INCLUDE_SOURCE_PREFIX_PATH
                    )
                    .define(
                            FAILURES_AWS_BUCKET_NAME_CONFIG,
                            ConfigDef.Type.STRING,
                            null,
                            ConfigDef.Importance.HIGH,
                            FAILURES_AWS_BUCKET_NAME_DOC,
                            CONFIG_GROUP,
                            groupCounter++,
                            ConfigDef.Width.NONE,
                            FAILURES_AWS_BUCKET_NAME_CONFIG
                    )
                    .define(
                            FAILURES_AWS_PREFIX_PATH_CONFIG,
                            ConfigDef.Type.STRING,
                            ConfigDef.NO_DEFAULT_VALUE,
                            ConfigDef.Importance.HIGH,
                            FAILURES_AWS_PREFIX_PATH_DOC,
                            CONFIG_GROUP,
                            groupCounter++,
                            ConfigDef.Width.NONE,
                            FAILURES_AWS_PREFIX_PATH_CONFIG
                    )
                    .define(
                            FAILURES_AWS_INCLUDE_SOURCE_PREFIX_PATH,
                            ConfigDef.Type.BOOLEAN,
                            false,
                            ConfigDef.Importance.LOW,
                            FAILURES_AWS_INCLUDE_SOURCE_PREFIX_PATH_DOC,
                            CONFIG_GROUP,
                            groupCounter++,
                            ConfigDef.Width.NONE,
                            FAILURES_AWS_INCLUDE_SOURCE_PREFIX_PATH
                    )
                    .define(
                            EXCLUDE_SOURCE_PREFIX_PATH_CONFIG,
                            ConfigDef.Type.STRING,
                            null,
                            ConfigDef.Importance.LOW,
                            EXCLUDE_SOURCE_PREFIX_PATH_DOC,
                            CONFIG_GROUP,
                            groupCounter++,
                            ConfigDef.Width.NONE,
                            EXCLUDE_SOURCE_PREFIX_PATH_CONFIG
                    );
        }
    }
}
