/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.internal;

import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Context class which is aware of system environment variables and runtime properties.
 */
public class Environment {

    private final SystemContext system;

    /**
     * Creates a new {@link Environment} instance.
     */
    public Environment() {
        system = new SystemContext(System.getenv(), System.getProperties());
    }

    public SystemContext system() {
        return system;
    }

    public static class SystemContext {

        private final Map<String, String> env;
        private final Map<String, String> props;

        /**
         * Creates a new {@link SystemContext} instance.
         *
         * @param env       the system environment variables.
         * @param props     the system environment properties.
         */
        SystemContext(final Map<String, String> env,
                      final Properties props) {
            this.env = env;
            this.props = fromProperties(props);
        }

        public Map<String, String> getEnv() {
            return env;
        }

        public Map<String, String> getProps() {
            return props;
        }

        private static Map<String, String> fromProperties(final Properties props) {
            Map<String, String> builder = new LinkedHashMap<>();
            for (Enumeration<?> e = props.propertyNames(); e.hasMoreElements(); ) {
                String key = (String) e.nextElement();
                builder.put(key, props.getProperty(key));
            }
            return builder;
        }
    }
}