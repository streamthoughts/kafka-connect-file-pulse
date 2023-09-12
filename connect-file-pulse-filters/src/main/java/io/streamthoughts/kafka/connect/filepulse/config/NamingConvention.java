/*
 * Copyright 2023 StreamThoughts.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
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