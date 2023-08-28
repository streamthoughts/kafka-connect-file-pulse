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

import static io.streamthoughts.kafka.connect.filepulse.config.ExtractValueConfig.EXTRACT_TARGET_CONFIG;
import static io.streamthoughts.kafka.connect.filepulse.config.ExtractValueConfig.REGEX_CONFIG;
import static io.streamthoughts.kafka.connect.filepulse.config.ExtractValueConfig.REGEX_DEFAULT_VALUE_CONFIG;
import static io.streamthoughts.kafka.connect.filepulse.config.ExtractValueConfig.REGEX_FIELD_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

class ExtractValueConfigTest {


    @Test
    void when_all_fields_config_specified_config_ok() {
        ExtractValueConfig config = new ExtractValueConfig(Map.of(
                REGEX_FIELD_CONFIG, Fixture.fieldName,
                REGEX_CONFIG, Fixture.regex,
                REGEX_DEFAULT_VALUE_CONFIG, Fixture.defaultValue,
                EXTRACT_TARGET_CONFIG, Fixture.targetName
        ));
        assertEquals(Fixture.fieldName, config.getFieldName());
        assertEquals(Fixture.regex, config.pattern().pattern());
        assertEquals(Fixture.defaultValue, config.getDefaultValue());
        assertEquals(Fixture.targetName, config.getTargetName());
    }

    @Test
    void when_regex_field_config_missing_exception_expected() {
        ConfigException configException = assertThrows(
                ConfigException.class,
                () -> new ExtractValueConfig(Map.of(
                        REGEX_CONFIG, Fixture.regex,
                        REGEX_DEFAULT_VALUE_CONFIG, Fixture.defaultValue)));
        assertTrue(configException.getMessage().contains(REGEX_FIELD_CONFIG));
    }

    @Test
    void when_regex_config_missing_exception_expected() {
        ConfigException configException = assertThrows(
                ConfigException.class,
                () -> new ExtractValueConfig(Map.of(
                        REGEX_FIELD_CONFIG, Fixture.fieldName,
                        REGEX_DEFAULT_VALUE_CONFIG, Fixture.defaultValue))
        );
        assertTrue(configException.getMessage().contains(REGEX_CONFIG));
    }

    @Test
    void when_default_value_config_field_config_missing_null_expected() {
        ExtractValueConfig config = new ExtractValueConfig(Map.of(
                REGEX_FIELD_CONFIG, Fixture.fieldName,
                REGEX_CONFIG, Fixture.regex));
        assertEquals(Fixture.fieldName, config.getFieldName());
        assertEquals(Fixture.regex, config.pattern().pattern());
        assertNull(config.getDefaultValue());
        assertNull(config.getTargetName());
    }

    @Test
    void when_target_field_config_missing_null_expected() {
        ExtractValueConfig config = new ExtractValueConfig(Map.of(
                REGEX_FIELD_CONFIG, Fixture.fieldName,
                REGEX_CONFIG, Fixture.regex,
                REGEX_DEFAULT_VALUE_CONFIG, Fixture.defaultValue
        ));
        assertEquals(Fixture.fieldName, config.getFieldName());
        assertEquals(Fixture.regex, config.pattern().pattern());
        assertEquals(Fixture.defaultValue, config.getDefaultValue());
        assertNull(config.getTargetName());
    }

    interface Fixture {
        String fieldName = "fieldA";
        String regex = "[a-z]";
        String defaultValue = "default";
        String targetName = "targetA";
    }
}