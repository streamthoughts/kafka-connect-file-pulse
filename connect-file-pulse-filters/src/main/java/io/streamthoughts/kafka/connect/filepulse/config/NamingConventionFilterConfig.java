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
package io.streamthoughts.kafka.connect.filepulse.config;

import static io.streamthoughts.kafka.connect.filepulse.config.NamingConvention.CAMEL_CASE;

import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;


public class NamingConventionFilterConfig extends AbstractConfig {
    public static final String CSV_DEFAULT_COLUMN_RENAME_STRATEGY_CONFIG = "csv.default.column.header.rename.strategy";
    public static final String CSV_DEFAULT_COLUMN_RENAME_DELIMITER_CONFIG = "csv.default.column.header.rename.delimiter";
    private static final String CSV_DEFAULT_COLUMN_RENAME_STRATEGY_DOC = "Default csv column header rename strategy, possible flavours are: camelCase, snakeCase, pascalCase";
    private static final String CSV_DEFAULT_COLUMN_RENAME_DELIMITER_DOC_TEMPLATE = "Set of characters to determine capitalization of the header, default value is %s";

    private static final String defaultDelimiter = "_ ,-()[]{}";

    public NamingConventionFilterConfig(Map<?, ?> originals) {
        super(getConfigDef(), originals);
    }

    public String getDefaultRenameStrategy() {
        return getString(CSV_DEFAULT_COLUMN_RENAME_STRATEGY_CONFIG);
    }

    public char[] getColumnHeaderDelimiters() {
        return getString(CSV_DEFAULT_COLUMN_RENAME_DELIMITER_CONFIG).toCharArray();
    }

    public static ConfigDef getConfigDef() {
        return new ConfigDef()
                .define(CSV_DEFAULT_COLUMN_RENAME_STRATEGY_CONFIG,
                        ConfigDef.Type.STRING,
                        CAMEL_CASE.getConfigValue(),
                        ConfigDef.Importance.HIGH,
                        CSV_DEFAULT_COLUMN_RENAME_STRATEGY_DOC)
                .define(CSV_DEFAULT_COLUMN_RENAME_DELIMITER_CONFIG,
                        ConfigDef.Type.STRING,
                        defaultDelimiter,
                        ConfigDef.Importance.HIGH,
                        String.format(CSV_DEFAULT_COLUMN_RENAME_DELIMITER_DOC_TEMPLATE, defaultDelimiter));
    }
}
