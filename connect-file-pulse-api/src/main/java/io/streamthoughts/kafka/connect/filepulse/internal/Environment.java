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