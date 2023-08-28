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

import static io.streamthoughts.kafka.connect.filepulse.config.MoveAllFieldsFilterConfig.MOVE_EXCLUDES_CONFIG;
import static io.streamthoughts.kafka.connect.filepulse.config.MoveAllFieldsFilterConfig.MOVE_TARGET_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Map;
import org.junit.jupiter.api.Test;

class MoveAllFieldsFilterConfigTest {

    @Test
    void when_target_config_omitted_default_value_is_used() {
        MoveAllFieldsFilterConfig config = new MoveAllFieldsFilterConfig(Map.of());
        assertEquals(Fixture.defaultPayload, config.target());
    }

    @Test
    void when_target_config_specified_that_value_is_used() {
        MoveAllFieldsFilterConfig config = new MoveAllFieldsFilterConfig(Map.of(MOVE_TARGET_CONFIG, Fixture.overriddenPayloadName));
        assertEquals(Fixture.overriddenPayloadName, config.target());
    }

    @Test
    void when_excludes_config_omitted_excludes_function_should_return_empty_collection() {
        MoveAllFieldsFilterConfig config = new MoveAllFieldsFilterConfig(Map.of());
        assertNotNull(config.excludes());
        assertTrue(config.excludes().isEmpty());
    }

    @Test
    void when_excludes_config_specified_with_one_field_excludes_function_should_return_collection_one_element() {
        MoveAllFieldsFilterConfig config = new MoveAllFieldsFilterConfig(Map.of(MOVE_EXCLUDES_CONFIG, Fixture.excludes0));
        assertNotNull(config.excludes());
        assertEquals(1, config.excludes().size());
        assertIterableEquals(Arrays.asList("excluded0"), config.excludes());
    }


    @Test
    void when_excludes_config_specified_with_two_fields_excludes_function_should_return_collection_two_elements() {
        MoveAllFieldsFilterConfig config = new MoveAllFieldsFilterConfig(Map.of(MOVE_EXCLUDES_CONFIG, Fixture.excludes1));
        assertNotNull(config.excludes());
        assertEquals(2, config.excludes().size());
        assertIterableEquals(Arrays.asList("excluded0", "excluded1"), config.excludes());
    }

    interface Fixture {
        String defaultPayload = "payload";
        String overriddenPayloadName = "pld";

        String excludes0 = "excluded0";
        String excludes1 = "excluded0,excluded1";
    }
}