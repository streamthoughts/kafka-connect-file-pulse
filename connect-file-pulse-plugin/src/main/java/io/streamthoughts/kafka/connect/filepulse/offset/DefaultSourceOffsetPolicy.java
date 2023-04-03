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
package io.streamthoughts.kafka.connect.filepulse.offset;

import static io.streamthoughts.kafka.connect.filepulse.source.LocalFileObjectMeta.SYSTEM_FILE_INODE_META_KEY;

import io.streamthoughts.kafka.connect.filepulse.annotation.VisibleForTesting;
import io.streamthoughts.kafka.connect.filepulse.errors.ConnectFilePulseException;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectMeta;
import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultSourceOffsetPolicy extends AbstractSourceOffsetPolicy {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultSourceOffsetPolicy.class);

    private static final String FILEPATH_FIELD = "path";
    private static final String FILENAME_FIELD = "name";
    private static final String URI_FIELD = "uri";
    private static final String INODE_FIELD = "inode";
    private static final String HASH_FIELD = "hash";
    private static final String MODIFIED_FIELD = "lastmodified";

    private final static Map<String, GenericOffsetPolicy> ATTRIBUTES = new HashMap<>();

    static {
        int priority = 1;
        ATTRIBUTES.put(
                FILENAME_FIELD,
                new GenericOffsetPolicy(
                        FILENAME_FIELD,
                        priority++,
                        FileObjectMeta::name
                )
        );
        ATTRIBUTES.put(
                FILEPATH_FIELD,
                new GenericOffsetPolicy(
                        FILEPATH_FIELD,
                        priority++,
                        objectMeta -> new File(objectMeta.uri()).getParentFile().getAbsolutePath()
                )
        );
        ATTRIBUTES.put(
                HASH_FIELD,
                new GenericOffsetPolicy(
                        HASH_FIELD,
                        priority++,
                        objectMeta -> {
                            return Optional.ofNullable(objectMeta.contentDigest())
                                    .map(FileObjectMeta.ContentDigest::digest)
                                    .orElseThrow(() -> new IllegalArgumentException(
                                        "Object file property 'content-digest' is empty")
                                    );
                        }
                )
        );
        ATTRIBUTES.put(
                MODIFIED_FIELD,
                new GenericOffsetPolicy(
                        MODIFIED_FIELD,
                        priority++,
                        objectMeta -> {
                            return Optional.ofNullable(objectMeta.lastModified())
                                    .orElseThrow(() -> new IllegalArgumentException(
                                        "Object file property 'last-modified' is empty")
                                    );
                        }
                )
        );
        ATTRIBUTES.put(
                URI_FIELD,
                new GenericOffsetPolicy(
                        URI_FIELD,
                        priority++,
                        FileObjectMeta::stringURI
                )
        );
        ATTRIBUTES.put(
                INODE_FIELD,
                new GenericOffsetPolicy(
                        INODE_FIELD,
                        priority++,
                        source -> Optional
                            .ofNullable(source.userDefinedMetadata().get(SYSTEM_FILE_INODE_META_KEY).toString())
                            .orElseThrow(() -> {
                                throw new ConnectFilePulseException(
                                    "Object file property 'unix-inode' is empty. " +
                                    "Unix-inode maybe not supported. " +
                                    "Consider configuring a different value for " +
                                    "'" + DefaultSourceOffsetPolicyConfig.OFFSET_ATTRIBUTES_STRING_CONFIG + "' " +
                                    "[path, name, hash, name, uri]"
                                );
                            }))
        );
    }

    protected final List<GenericOffsetPolicy> policies = new LinkedList<>();

    private String offsetAttributesString;

    /**
     * Creates a new {@link DefaultSourceOffsetPolicy} instance.
     */
    public DefaultSourceOffsetPolicy() { }

    /**
     * Creates a new {@link DefaultSourceOffsetPolicy} instance.
     *
     * @param offsetAttributesString    {@code offset.attributes.string}
     */
    @VisibleForTesting
    DefaultSourceOffsetPolicy(final String offsetAttributesString) {
        this.offsetAttributesString = offsetAttributesString;
        parseConfig(offsetAttributesString);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(final Map<String, ?> configs) {
        offsetAttributesString = new DefaultSourceOffsetPolicyConfig(configs).offsets();
        parseConfig(offsetAttributesString);
    }

    private void parseConfig(final String offsetStrategyString) {
        for (String label : offsetStrategyString.split("\\+")) {
            final GenericOffsetPolicy strategy = ATTRIBUTES.get(label.toLowerCase());
            if (strategy == null) {
                throw new IllegalArgumentException("Unknown offset policy for name '" + label + "'");
            }
            this.policies.add(strategy);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, Object> toPartitionMap(final FileObjectMeta objectMeta) {
        Collections.sort(policies);
        Map<String, Object> offset = new LinkedHashMap<>();
        for (GenericOffsetPolicy policy : policies) {
            try {
                policy.addAttributeToPartitionMap(objectMeta, offset);
            } catch (Exception e) {
                LOG.error(
                    "Unexpected error while building partition map using policy '{}'. Error: {}",
                    policy.name,
                    e.getMessage()
                );
                throw e;
            }
        }
        return offset;
    }

    static final class GenericOffsetPolicy implements Comparable<GenericOffsetPolicy> {

        final String name;
        final Function<FileObjectMeta, Object> offsetFunction;
        final int priority;

        GenericOffsetPolicy(final String name,
                            final int priority,
                            final Function<FileObjectMeta, Object> offsetFunction) {
            this.name = Objects.requireNonNull(name, "name cannot be null");
            this.offsetFunction = Objects.requireNonNull(offsetFunction, "offsetFunction cannot be null");
            this.priority = priority;
        }

        void addAttributeToPartitionMap(final FileObjectMeta objectMeta,
                                        final Map<String, Object> offset) {
            offset.put(name, offsetFunction.apply(objectMeta));
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public int compareTo(final GenericOffsetPolicy that) {
            return Integer.compare(this.priority, that.priority);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return offsetAttributesString;
    }
}
