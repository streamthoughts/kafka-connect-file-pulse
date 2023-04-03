/*
 * Copyright 2023 StreamThoughts.
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

import io.streamthoughts.kafka.connect.filepulse.annotation.VisibleForTesting;
import io.streamthoughts.kafka.connect.filepulse.clean.FileCleanupPolicy;
import io.streamthoughts.kafka.connect.filepulse.config.SimpleConfig;
import io.streamthoughts.kafka.connect.filepulse.fs.Storage;
import io.streamthoughts.kafka.connect.filepulse.source.FileObject;
import java.net.URI;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Policy for printing into log files completed files.
 */
public final class RegexRouterCleanupPolicy implements FileCleanupPolicy {

    private static final String CONFIG_PREFIX = "fs.cleanup.policy.router.";

    public static final String SUCCESS_ROUTE_TOPIC_REGEX_CONFIG = CONFIG_PREFIX + "success.uri.regex";
    private static final String SUCCESS_ROUTE_TOPIC_REGEX_DOC =
            "Regular expression to use for matching objects in success.";
    public static final String SUCCESS_ROUTE_TOPIC_REPLACEMENT_CONFIG = CONFIG_PREFIX + "success.uri.replacement";
    private static final String SUCCESS_ROUTE_TOPIC_REPLACEMENT_DOC = "Replacement string.";

    public static final String FAILURE_ROUTE_TOPIC_REGEX_CONFIG = CONFIG_PREFIX + "failure.uri.regex";
    private static final String FAILURE_ROUTE_TOPIC_REGEX_DOC =
            "Regular expression to use for matching objects in failure.";
    public static final String FAILURE_ROUTE_TOPIC_REPLACEMENT_CONFIG = CONFIG_PREFIX + "failure.uri.replacement";
    private static final String FAILURE_ROUTE_TOPIC_REPLACEMENT_DOC = "Replacement string.";

    private static final Logger LOG = LoggerFactory.getLogger(LogCleanupPolicy.class);

    private String successReplacement;
    private Pattern successRegex;

    private String failureReplacement;
    private Pattern failureRegex;

    private Storage storage;

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(final Map<String, ?> configs) {
        SimpleConfig simpleConfig = new SimpleConfig(configDef(), configs);
        successReplacement = simpleConfig.getString(SUCCESS_ROUTE_TOPIC_REPLACEMENT_CONFIG);
        successRegex = Pattern.compile(simpleConfig.getString(SUCCESS_ROUTE_TOPIC_REGEX_CONFIG));

        failureReplacement = simpleConfig.getString(SUCCESS_ROUTE_TOPIC_REPLACEMENT_CONFIG);
        failureRegex = Pattern.compile(simpleConfig.getString(SUCCESS_ROUTE_TOPIC_REGEX_CONFIG));
    }

    /**
     * {@inheritDoc}
     */
    public boolean onSuccess(final FileObject source) {
        URI sourceURI = source.metadata().uri();
        return storage.move(sourceURI, routeOnSuccess(sourceURI));
    }

    /**
     * {@inheritDoc}
     */
    public boolean onFailure(final FileObject source) {
        URI sourceURI = source.metadata().uri();
        return storage.move(sourceURI, routeOnFailure(sourceURI));
    }

    @VisibleForTesting
    URI routeOnSuccess(URI sourceURI) {
        return route(sourceURI.toString(), successReplacement, successRegex);
    }

    @VisibleForTesting
    URI routeOnFailure(URI sourceURI) {
        return route(sourceURI.toString(), failureReplacement, failureRegex);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
    }

    @VisibleForTesting
    private URI route(final String originalURI,
                      final String replacement,
                      final Pattern regex) {
        final Matcher matcher = regex.matcher(originalURI);

        String targetURI;
        if (matcher.matches()) {
            targetURI = matcher.replaceFirst(replacement);
            LOG.trace("Rerouting from object-file from '{}' to '{}'", originalURI, targetURI);
        } else {
            targetURI = originalURI;
            LOG.trace("Not rerouting object-file '{}' as it does not match the configured regex", originalURI);
        }

        return URI.create(targetURI);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setStorage(final Storage storage) {
        this.storage = storage;
    }

    private static ConfigDef configDef() {
        int orderInGroup = 0;
        return new ConfigDef()
                .define(
                        SUCCESS_ROUTE_TOPIC_REPLACEMENT_CONFIG,
                        ConfigDef.Type.STRING,
                        "${routedByValue}",
                        new ConfigDef.NonEmptyString(),
                        ConfigDef.Importance.HIGH,
                        SUCCESS_ROUTE_TOPIC_REPLACEMENT_DOC,
                        null,
                        orderInGroup,
                        ConfigDef.Width.NONE,
                        SUCCESS_ROUTE_TOPIC_REPLACEMENT_CONFIG)
                .define(
                        SUCCESS_ROUTE_TOPIC_REGEX_CONFIG,
                        ConfigDef.Type.STRING,
                        "(?<routedByValue>.*)",
                        new ConfigDef.NonEmptyString(),
                        ConfigDef.Importance.HIGH,
                        SUCCESS_ROUTE_TOPIC_REGEX_DOC,
                        null,
                        orderInGroup,
                        ConfigDef.Width.NONE,
                        SUCCESS_ROUTE_TOPIC_REGEX_CONFIG
                )
                .define(
                        FAILURE_ROUTE_TOPIC_REPLACEMENT_CONFIG,
                        ConfigDef.Type.STRING,
                        "${routedByValue}",
                        new ConfigDef.NonEmptyString(),
                        ConfigDef.Importance.HIGH,
                        FAILURE_ROUTE_TOPIC_REPLACEMENT_DOC,
                        null,
                        orderInGroup,
                        ConfigDef.Width.NONE,
                        FAILURE_ROUTE_TOPIC_REPLACEMENT_CONFIG)
                .define(
                        FAILURE_ROUTE_TOPIC_REGEX_CONFIG,
                        ConfigDef.Type.STRING,
                        "(?<routedByValue>.*)",
                        new ConfigDef.NonEmptyString(),
                        ConfigDef.Importance.HIGH,
                        FAILURE_ROUTE_TOPIC_REGEX_DOC,
                        null,
                        orderInGroup,
                        ConfigDef.Width.NONE,
                        FAILURE_ROUTE_TOPIC_REGEX_CONFIG
                );
    }
}