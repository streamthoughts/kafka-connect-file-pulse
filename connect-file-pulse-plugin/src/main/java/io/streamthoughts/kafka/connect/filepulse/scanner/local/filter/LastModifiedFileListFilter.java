/*
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
package io.streamthoughts.kafka.connect.filepulse.scanner.local.filter;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.attribute.FileTime;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class LastModifiedFileListFilter extends AbstractFileListFilter {

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
    protected boolean accept(final File file) {
        boolean accepted = isNotModifiedForMs(file, minimumAgeMs);
        if (!accepted) {
            LOG.debug("Filtering file {} - doesn't matches minimum age of {}.", file.getName(), minimumAgeMs);
        }

        return accepted;
    }

    private static ConfigDef getConfigDef() {
        return new ConfigDef()
                .define(FILE_MINIMUM_AGE_MS_CONF, ConfigDef.Type.LONG, FILE_MINIMUM_AGE_MS_DEFAULT,
                        ConfigDef.Importance.HIGH, FILE_MINIMUM_AGE_MS_DOC);
    }

    private static boolean isNotModifiedForMs(final File f, final long minimumAgeMs) {
        try {
            final FileTime lastModifiedTime = Files.getLastModifiedTime(f.toPath(), LinkOption.NOFOLLOW_LINKS);
            long lastModifiedTimeMs = lastModifiedTime.to(TimeUnit.MILLISECONDS);
            long currentTimeMs = System.currentTimeMillis();
            return currentTimeMs - lastModifiedTimeMs > minimumAgeMs;
        } catch (IOException e) {
            return false;
        }
    }
}
