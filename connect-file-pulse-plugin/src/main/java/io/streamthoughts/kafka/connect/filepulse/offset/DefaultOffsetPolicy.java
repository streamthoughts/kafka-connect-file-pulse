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
import java.util.function.Function;

import static io.streamthoughts.kafka.connect.filepulse.source.LocalFileObjectMeta.SYSTEM_FILE_INODE_META_KEY;

public class DefaultOffsetPolicy extends AbstractSourceOffsetPolicy {

    private static final String FILEPATH_FIELD = "path";
    private static final String FILENAME_FIELD = "name";
    private static final String URI_FIELD      = "uri";
    private static final String INODE_FIELD    = "inode";
    private static final String HASH_FIELD     = "hash";
    private static final String MODIFIED_FIELD = "lastmodified";

    private final static Map<String, GenericOffsetPolicy> ATTRIBUTES = new HashMap<>();

    static  {
        ATTRIBUTES.put(FILENAME_FIELD, new GenericOffsetPolicy(FILENAME_FIELD,1, FileObjectMeta::name));
        ATTRIBUTES.put(FILEPATH_FIELD, new GenericOffsetPolicy(FILEPATH_FIELD, 2,  source -> {
            return new File(source.uri()).getParentFile().getAbsolutePath();
        }));
        ATTRIBUTES.put(HASH_FIELD, new GenericOffsetPolicy(HASH_FIELD, 3, source -> {
            return source.contentDigest().digest();
        }));
        ATTRIBUTES.put(MODIFIED_FIELD, new GenericOffsetPolicy(MODIFIED_FIELD, 4, FileObjectMeta::lastModified));
        ATTRIBUTES.put(URI_FIELD, new GenericOffsetPolicy(URI_FIELD, 5, FileObjectMeta::stringURI));
        ATTRIBUTES.put(INODE_FIELD, new GenericOffsetPolicy(INODE_FIELD, 6, source -> {
            final String inode = (String)source.userDefinedMetadata().get(SYSTEM_FILE_INODE_META_KEY);
            if (inode == null) {
                throw new ConnectFilePulseException(
                    "Unix-inode is not supported. " +
                    "Consider configuring a different 'offset-strategy' [path, name, hash, name, uri]");
            }
            return inode;
        }));
    }

    protected final List<GenericOffsetPolicy> policies = new LinkedList<>();

    private String offsetAttributesString;

    /**
     * Creates a new {@link DefaultOffsetPolicy} instance.
     */
    public DefaultOffsetPolicy() {
    }

    @VisibleForTesting
    DefaultOffsetPolicy(final String offsetStrategyString) {
        this.offsetAttributesString = offsetStrategyString;
        parseConfig(offsetStrategyString);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(final Map<String, ?> configs) {
        offsetAttributesString = new DefaultOffsetPolicyConfig(configs).offsets();
        parseConfig(offsetAttributesString);
    }

    private void parseConfig(final String offsetStrategyString) {
        for (String label : offsetStrategyString.split("\\+")) {
            final GenericOffsetPolicy strategy = ATTRIBUTES.get(label.toLowerCase());
            if (strategy == null) {
                throw new IllegalArgumentException("Unknown offset strategy for name '" + label + "'");
            }
            this.policies.add(strategy);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, Object> toPartitionMap(final FileObjectMeta source) {
        Collections.sort(policies);
        Map<String, Object> offset = new LinkedHashMap<>();
        for (GenericOffsetPolicy strategy : policies) {
            strategy.addAttributeToPartitionMap(source, offset);
        }
        return offset;
    }

    static class GenericOffsetPolicy implements Comparable<GenericOffsetPolicy> {

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

        void addAttributeToPartitionMap(final FileObjectMeta object, final Map<String, Object> offset) {
            offset.put(name, offsetFunction.apply(object));
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
