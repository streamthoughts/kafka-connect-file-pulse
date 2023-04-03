/*
 * Copyright 2019-2020 StreamThoughts.
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
                .define(FILE_FILTER_REGEX_PATTERN_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
                        FILE_FILTER_REGEX_PATTERN_DOC);
    }
}