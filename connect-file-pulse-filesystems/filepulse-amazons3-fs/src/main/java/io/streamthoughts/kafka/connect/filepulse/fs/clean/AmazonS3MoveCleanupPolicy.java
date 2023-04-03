/*
 * Copyright 2022 StreamThoughts.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.streamthoughts.kafka.connect.filepulse.fs.clean;

import static io.streamthoughts.kafka.connect.filepulse.fs.clean.AmazonS3MoveCleanupPolicy.Config.FAILURES_AWS_BUCKET_NAME_CONFIG;
import static io.streamthoughts.kafka.connect.filepulse.fs.clean.AmazonS3MoveCleanupPolicy.Config.FAILURES_AWS_PREFIX_PATH_CONFIG;
import static io.streamthoughts.kafka.connect.filepulse.fs.clean.AmazonS3MoveCleanupPolicy.Config.SUCCESS_AWS_BUCKET_NAME_CONFIG;
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

    private Config config;

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(final Map<String, ?> configs) {
        this.config = new Config(configs);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean onSuccess(final FileObject source) {
        return move(source, SUCCESS_AWS_BUCKET_NAME_CONFIG, SUCCESS_AWS_PREFIX_PATH_CONFIG);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean onFailure(final FileObject source) {
        return move(source, FAILURES_AWS_BUCKET_NAME_CONFIG, FAILURES_AWS_PREFIX_PATH_CONFIG);
    }

    private boolean move(final FileObject source,
                         final String destinationS3BucketConfig,
                         final String destinationS3PrefixConfig) {
        checkState();
        URI sourceURI = source.metadata().uri();
        if (!storage.exists(sourceURI)) {
            LOG.warn("Cannot move object-file '{}' to failure S3 bucket due to file does not exist.", sourceURI);
            return true;
        }
        S3BucketKey sourceBucketKey = S3BucketKey.fromURI(sourceURI);

        var destS3BucketName = Optional
                .ofNullable(config.getString(destinationS3BucketConfig))
                .orElse(sourceBucketKey.bucketName());

        var destBucketKey = new S3BucketKey(
                destS3BucketName,
                config.getString(destinationS3PrefixConfig),
                sourceBucketKey.objectName()
        );
        return storage.move(sourceURI, destBucketKey.toURI());
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

        public static final String FAILURES_AWS_BUCKET_NAME_CONFIG =
                CONFIG_PREFIX + "failure.aws.bucket.name";
        private static final String FAILURES_AWS_BUCKET_NAME_DOC =
                "The name of the destination S3 bucket for failure objects.";

        public static final String FAILURES_AWS_PREFIX_PATH_CONFIG =
                CONFIG_PREFIX + "failure.aws.prefix.path";
        private static final String FAILURES_AWS_PREFIX_PATH_DOC =
                "The prefix to be used for defining the key of S3 object to move into the destination bucket.";

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
                    );
        }
    }
}
