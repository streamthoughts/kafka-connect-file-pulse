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
package io.streamthoughts.kafka.connect.filepulse.reader;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

/**
 * Configuration class for {@link RowFileInputReader}.
 */
public class XMLFileInputReaderConfig extends AbstractConfig {

    public static final String XPATH_QUERY_CONFIG  = "xpath.expression";
    public static final String XPATH_QUERY_DOC     = "The XPath expression used to split the XML into a list of nodes";

    /**
     * Creates a new {@link XMLFileInputReaderConfig} instance.
     *
     * @param originals the reader configuration.
     */
    XMLFileInputReaderConfig(final Map<String, ?> originals) {
        super(configDef(), originals);
    }

    String xpathQuery() {
        return getString(XPATH_QUERY_CONFIG);
    }

    private static ConfigDef configDef() {
        return new ConfigDef()
                .define(XPATH_QUERY_CONFIG, ConfigDef.Type.STRING,
                        ConfigDef.Importance.HIGH, XPATH_QUERY_DOC);
    }
}
