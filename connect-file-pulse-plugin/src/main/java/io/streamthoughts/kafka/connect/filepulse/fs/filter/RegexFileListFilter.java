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
import java.util.regex.Pattern;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class RegexFileListFilter extends PredicateFileListFilter {

    private static final Logger LOG = LoggerFactory.getLogger(RegexFileListFilter.class);

    private final static String GROUP = "RegexFileListFilter";

    private static final String FILE_FILTER_REGEX_PATTERN_CONFIG = "file.filter.regex.pattern";
    private static final String FILE_FILTER_REGEX_PATTERN_DOC    = "The regex pattern used to matches input files";

    private Pattern pattern;

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(final Map<String, ?> config) {
        AbstractConfig abstractConfig = new AbstractConfig(getConfigDef(), config, false);
        String pattern = abstractConfig.getString(FILE_FILTER_REGEX_PATTERN_CONFIG);
        if (pattern == null) {
            throw new ConfigException("missing configuration: " + FILE_FILTER_REGEX_PATTERN_CONFIG);
        }
        setPattern(pattern);
    }

    private void setPattern(String pattern) {
        this.pattern = Pattern.compile(pattern);
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public boolean test(final FileObjectMeta meta) {
        if (meta == null) return false;

        boolean matched = pattern.matcher(meta.name()).matches();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Matching input file name {} to regex {}, matched = {}", meta.name(), pattern, matched);
        }
        return matched;
    }

    private static ConfigDef getConfigDef() {
        return new ConfigDef()
                .define(
                        FILE_FILTER_REGEX_PATTERN_CONFIG,
                        ConfigDef.Type.STRING,
                        ConfigDef.NO_DEFAULT_VALUE,
                        new ConfigDef.NonEmptyString(),
                        ConfigDef.Importance.HIGH,
                        FILE_FILTER_REGEX_PATTERN_DOC,
                        GROUP,
                        0,
                        ConfigDef.Width.NONE,
                        FILE_FILTER_REGEX_PATTERN_CONFIG
                );
    }
}