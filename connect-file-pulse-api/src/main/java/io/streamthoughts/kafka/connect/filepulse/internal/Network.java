/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.internal;

import java.net.InetAddress;
import java.net.UnknownHostException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class Network {

    private static final Logger LOG = LoggerFactory.getLogger(Network.class);

    private static final String DEFAULT_LOCALHOST = "localhost";

    public static final String HOSTNAME = getLocalHostName();

    private static String getLocalHostName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            LOG.error("Cannot retrieve local hostname: {}", e.getMessage());
        }
        return DEFAULT_LOCALHOST;
    }
}