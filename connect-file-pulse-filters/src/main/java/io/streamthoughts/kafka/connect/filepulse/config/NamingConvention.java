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

import java.util.Arrays;
import org.apache.kafka.common.config.ConfigException;

public enum NamingConvention {
    CAMEL_CASE("camelCase"),
    PASCAL_CASE("pascalCase"),
    SNAKE_CASE("snakeCase");
    private static final String RENAME_STRATEGY_NOT_FOUND_ERROR_MSG_TEMPLATE = "Rename strategy: %s does not exist";
    private final String configValue;

    NamingConvention(String configValue) {
        this.configValue = configValue;
    }

    public static NamingConvention getByConfigValue(String searchedConfigValue) {
        return Arrays.stream(values())
                .filter(e -> e.configValue.equals(searchedConfigValue))
                .findAny()
                .orElseThrow(() -> new ConfigException(buildRenameStrategyNotFoundErrorMsg(searchedConfigValue)));
    }

    public String getConfigValue() {
        return configValue;
    }

    public static String buildRenameStrategyNotFoundErrorMsg(String strategyName) {
        return String.format(RENAME_STRATEGY_NOT_FOUND_ERROR_MSG_TEMPLATE, strategyName);
    }
}