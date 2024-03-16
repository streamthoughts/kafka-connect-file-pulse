/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.config;

import java.util.Arrays;
import org.apache.kafka.common.config.ConfigException;

public enum NamingConvention {
    CAMEL_CASE("camelCase"),
    PASCAL_CASE("pascalCase"),
    SNAKE_CASE("snakeCase");

    private static final String NAMING_CONVENTION_NOT_FOUND_ERROR_MSG_TEMPLATE = "Naming Convention: %s does not exist";
    private final String configValue;

    NamingConvention(String configValue) {
        this.configValue = configValue;
    }

    public static NamingConvention getByConfigValue(String searchedConfigValue) {
        return Arrays.stream(values())
                .filter(e -> e.configValue.equals(searchedConfigValue))
                .findAny()
                .orElseThrow(() -> new ConfigException(namingConventionNotFoundErrorMsg(searchedConfigValue)));
    }

    public String getConfigValue() {
        return configValue;
    }

    public static String namingConventionNotFoundErrorMsg(String strategyName) {
        return String.format(NAMING_CONVENTION_NOT_FOUND_ERROR_MSG_TEMPLATE, strategyName);
    }
}