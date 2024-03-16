/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.fs.filter;

import io.streamthoughts.kafka.connect.filepulse.fs.PredicateFileListFilter;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectMeta;
import java.util.Map;
import java.util.function.Predicate;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link PredicateFileListFilter} that allows excluding from processing files
 * that have not been modified since either a given maximum or minimum time in ms.
 */
public class LastModifiedFileListFilter extends PredicateFileListFilter {

    private final static String GROUP = "LastModifiedFileListFilter";

    private static final Logger LOG = LoggerFactory.getLogger(LastModifiedFileListFilter.class);

    public static final String FILE_MINIMUM_AGE_MS_CONFIG = "file.filter.minimum.age.ms";
    private static final String FILE_MINIMUM_AGE_MS_DOC =
            "The minimum age in milliseconds of a file to be eligible for processing.";
    private static final long FILE_MINIMUM_AGE_MS_DEFAULT = 0L;

    public static final String FILE_MAXIMUM_AGE_MS_CONFIG = "file.filter.maximum.age.ms";
    private static final String FILE_MAXIMUM_AGE_MS_DOC =
            "The maximum age in milliseconds of a file to be eligible for processing.";
    private static final long FILE_MAXIMUM_AGE_MS_DEFAULT = Long.MAX_VALUE;

    private Predicate<FileObjectMeta> minimumAgePredicate;

    private Predicate<FileObjectMeta> maximumAgePredicate;

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(final Map<String, ?> props) {
        final AbstractConfig abstractConfig = new AbstractConfig(getConfigDef(), props);
        final Long minimumAgeMs = abstractConfig.getLong(FILE_MINIMUM_AGE_MS_CONFIG);
        this.minimumAgePredicate = it -> Math.max(0, System.currentTimeMillis() - it.lastModified()) > minimumAgeMs;

        final Long maximumAgeMs = abstractConfig.getLong(FILE_MAXIMUM_AGE_MS_CONFIG);
        this.maximumAgePredicate = it -> Math.max(0, System.currentTimeMillis() - it.lastModified()) < maximumAgeMs;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean test(final FileObjectMeta meta) {

        if (!minimumAgePredicate.test(meta)) {
            LOG.debug(
                "Filtered '{}'. File do not match minimum age predicate.",
                meta
            );
            return false;
        }

        if (!maximumAgePredicate.test(meta)) {
            LOG.debug(
                "Filtered '{}'. File do not match maximum age predicate.",
                meta
            );
            return false;
        }

        return true;
    }

    private static ConfigDef getConfigDef() {
        int groupCounter = 0;
        return new ConfigDef()
                .define(
                        FILE_MINIMUM_AGE_MS_CONFIG,
                        ConfigDef.Type.LONG,
                        FILE_MINIMUM_AGE_MS_DEFAULT,
                        ConfigDef.Range.atLeast(0),
                        ConfigDef.Importance.HIGH,
                        FILE_MINIMUM_AGE_MS_DOC,
                        GROUP,
                        groupCounter++,
                        ConfigDef.Width.NONE,
                        FILE_MINIMUM_AGE_MS_CONFIG
                )
                .define(
                        FILE_MAXIMUM_AGE_MS_CONFIG,
                        ConfigDef.Type.LONG,
                        FILE_MAXIMUM_AGE_MS_DEFAULT,
                        ConfigDef.Range.atLeast(0),
                        ConfigDef.Importance.HIGH,
                        FILE_MAXIMUM_AGE_MS_DOC,
                        GROUP,
                        groupCounter++,
                        ConfigDef.Width.NONE,
                        FILE_MAXIMUM_AGE_MS_CONFIG
                );
    }
}
