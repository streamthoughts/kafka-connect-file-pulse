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
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 *
 */
public class LastModifiedFileListFilter extends PredicateFileListFilter {

    private static final String FILE_MINIMUM_AGE_MS_CONF    = "file.filter.minimum.age.ms";
    private static final String FILE_MINIMUM_AGE_MS_DOC     =
        "Last modified time for a file can be accepted (default: 5000)";
    private static final int FILE_MINIMUM_AGE_MS_DEFAULT    = 5000;
    private static final Logger LOG = LoggerFactory.getLogger(LastModifiedFileListFilter.class);

    private long minimumAgeMs;

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(final Map<String, ?> props) {
        AbstractConfig abstractConfig = new AbstractConfig(getConfigDef(), props);
        this.minimumAgeMs = abstractConfig.getLong(FILE_MINIMUM_AGE_MS_CONF);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean test(final FileObjectMeta meta) {
        boolean accepted = isNotModifiedForMs(meta, minimumAgeMs);
        if (!accepted) {
            LOG.debug("Filtering file {} - doesn't matches minimum age of {}.", meta.name(), minimumAgeMs);
        }

        return accepted;
    }

    private static ConfigDef getConfigDef() {
        return new ConfigDef()
                .define(FILE_MINIMUM_AGE_MS_CONF, ConfigDef.Type.LONG, FILE_MINIMUM_AGE_MS_DEFAULT,
                        ConfigDef.Importance.HIGH, FILE_MINIMUM_AGE_MS_DOC);
    }

    private static boolean isNotModifiedForMs(final FileObjectMeta meta, final long minimumAgeMs) {
        return System.currentTimeMillis() - meta.lastModified() > minimumAgeMs;
    }
}
