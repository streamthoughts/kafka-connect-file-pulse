/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse;

import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Version {

    private static final Logger LOG = LoggerFactory.getLogger(Version.class);

    private static String version = "unknown";

    static {
        try {
            Properties props = new Properties();
            props.load(Version.class.getResourceAsStream("/kafka-connect-source-file-pulse-version.properties"));
            version = props.getProperty("version", version).trim();
        } catch (Exception e) {
            LOG.warn("Error while loading version: ", e);
        }
    }

    public static String getVersion() {
        return version;
    }
}