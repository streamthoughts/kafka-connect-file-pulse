/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
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