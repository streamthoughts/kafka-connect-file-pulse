/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.fs;

import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

public class AmazonS3StorageConfig extends AbstractConfig {

    private static final String CONFIG_GROUP = "AmazonS3Storage";

    private static final String CONFIG_PREFIX = "fs.cleanup.policy.move.aws.";

    public static final String MULTIPART_COPY_THRESHOLD_CONFIG = CONFIG_PREFIX + "multipart.threshold";
    private static final String MULTIPART_COPY_THRESHOLD_CONFIG_DOC =
            "File size threshold in bytes above which a multipart copy will be used instead of a single-part copy";
    private static final long MULTIPART_COPY_THRESHOLD_CONFIG_DEFAULT = 1024*1024*1024*5L; //5 GB
    private static final ConfigDef.Validator MULTIPART_COPY_THRESHOLD_CONFIG_VALIDATOR
            = ConfigDef.Range.between(5 * 1024 * 1024L, 5L * 1024 * 1024 * 1024); // between 5 MB and 5 GB

    public static final String PART_SIZE_CONFIG = CONFIG_PREFIX + "parts.size";
    private static final String PART_SIZE_CONFIG_DOC = "Size in bytes of each part when performing a multipart copy";
    private static final long PART_SIZE_CONFIG_DEFAULT = 100*1024*1024; //100 MB
    private static final ConfigDef.Validator PART_SIZE_CONFIG_VALIDATOR = ConfigDef.Range.atLeast(5 * 1024 * 1024L);

    public static final String MAX_PARTS_CONFIG = CONFIG_PREFIX + "parts.max";
    private static final String MAX_PARTS_CONFIG_DOC = "Maximum number of parts allowed in a multipart copy operation";
    private static final int MAX_PARTS_CONFIG_DEFAULT = 10_000;
    private static final ConfigDef.Validator MAX_PARTS_CONFIG_VALIDATOR = ConfigDef.Range.between(1, 10_000);

    public long getMultipartCopyThresholdConfig() {
        return getLong(MULTIPART_COPY_THRESHOLD_CONFIG);
    }

    public long getPartSizeConfig() {
        return getLong(PART_SIZE_CONFIG);
    }

    public int getMaxPartsConfig() {
        return getInt(MAX_PARTS_CONFIG);
    }

    /**
     * Creates a new {@link AmazonS3StorageConfig} instance.
     */
    public AmazonS3StorageConfig(final Map<?, ?> originals) {
        super(configDef(), originals, true);
    }

    static ConfigDef configDef() {
        int groupCounter = 0;
        return new ConfigDef()
                .define(
                        MULTIPART_COPY_THRESHOLD_CONFIG,
                        ConfigDef.Type.LONG,
                        MULTIPART_COPY_THRESHOLD_CONFIG_DEFAULT,
                        MULTIPART_COPY_THRESHOLD_CONFIG_VALIDATOR,
                        ConfigDef.Importance.HIGH,
                        MULTIPART_COPY_THRESHOLD_CONFIG_DOC,
                        CONFIG_GROUP,
                        groupCounter++,
                        ConfigDef.Width.NONE,
                        MULTIPART_COPY_THRESHOLD_CONFIG
                )
                .define(
                        PART_SIZE_CONFIG,
                        ConfigDef.Type.LONG,
                        PART_SIZE_CONFIG_DEFAULT,
                        PART_SIZE_CONFIG_VALIDATOR,
                        ConfigDef.Importance.HIGH,
                        PART_SIZE_CONFIG_DOC,
                        CONFIG_GROUP,
                        groupCounter++,
                        ConfigDef.Width.NONE,
                        PART_SIZE_CONFIG
                )
                .define(
                        MAX_PARTS_CONFIG,
                        ConfigDef.Type.INT,
                        MAX_PARTS_CONFIG_DEFAULT,
                        MAX_PARTS_CONFIG_VALIDATOR,
                        ConfigDef.Importance.HIGH,
                        MAX_PARTS_CONFIG_DOC,
                        CONFIG_GROUP,
                        groupCounter++,
                        ConfigDef.Width.NONE,
                        MAX_PARTS_CONFIG
                );
    }
}
