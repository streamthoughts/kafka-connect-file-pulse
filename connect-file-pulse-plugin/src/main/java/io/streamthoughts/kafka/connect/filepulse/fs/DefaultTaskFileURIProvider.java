/*
 * Copyright 2021 StreamThoughts.
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
package io.streamthoughts.kafka.connect.filepulse.fs;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

public class DefaultTaskFileURIProvider implements TaskFileURIProvider {

    private List<URI> objectURIs;

    private final AtomicBoolean hasMore = new AtomicBoolean(true);

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(final Map<String, ?> configs) {
        objectURIs = new Config(configs).objectURIs();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<URI> nextURIs() {
        if (hasMore.compareAndSet(true, false)) {
            return objectURIs;
        } else {
            throw new NoSuchElementException("No more URIs can be retrieved from this provider.");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasMore() {
        return hasMore.get();
    }

    public static final class Config extends AbstractConfig {

        public static final String FILE_OBJECT_URIS_CONFIG = "file.object.uris";
        private static final String FILE_OBJECT_URIS_DOC = "The list of files task must proceed.";

        /**
         * Creates a new {@link Config} instance.
         *
         * @param originals the original configs.
         */
        public Config(final Map<String, ?> originals) {
            super(getConf(), originals, false);
        }

        static ConfigDef getConf() {
            return new ConfigDef()
                            .define(
                    FILE_OBJECT_URIS_CONFIG,
                    ConfigDef.Type.LIST,
                    ConfigDef.Importance.HIGH,
                    FILE_OBJECT_URIS_DOC
            );
        }

        public List<URI> objectURIs() {
            return this.getList(FILE_OBJECT_URIS_CONFIG).stream().map(URI::create).collect(Collectors.toList());
        }
    }
}
