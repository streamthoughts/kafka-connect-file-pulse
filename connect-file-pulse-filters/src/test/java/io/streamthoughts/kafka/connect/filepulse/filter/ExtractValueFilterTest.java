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
package io.streamthoughts.kafka.connect.filepulse.filter;

import static io.streamthoughts.kafka.connect.filepulse.config.ExtractValueConfig.EXTRACT_TARGET_CONFIG;
import static io.streamthoughts.kafka.connect.filepulse.config.ExtractValueConfig.REGEX_CONFIG;
import static io.streamthoughts.kafka.connect.filepulse.config.ExtractValueConfig.REGEX_DEFAULT_VALUE_CONFIG;
import static io.streamthoughts.kafka.connect.filepulse.config.ExtractValueConfig.REGEX_FIELD_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.reader.RecordsIterable;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ExtractValueFilterTest {

    @Test
    void when_record_null_apply_should_return_empty_iterable() {
        FilterContext context = mock(FilterContext.class);
        ExtractValueFilter filter = new ExtractValueFilter();
        filter.configure(Map.of(
                REGEX_FIELD_CONFIG, Fixture.fieldA,
                REGEX_CONFIG, "[a-z]",
                REGEX_DEFAULT_VALUE_CONFIG, Fixture.defaultValue));

        RecordsIterable<TypedStruct> res = filter.apply(context, null, false);
        assertTrue(res.isEmpty());
    }

    @Test
    void when_record_no_match_apply_should_return_field_with_default_value() {
        FilterContext context = mock(FilterContext.class);
        ExtractValueFilter filter = new ExtractValueFilter();
        filter.configure(Map.of(
                REGEX_FIELD_CONFIG, Fixture.fieldA,
                REGEX_CONFIG, "[a-z]",
                REGEX_DEFAULT_VALUE_CONFIG, Fixture.defaultValue));

        TypedStruct typedStruct = TypedStruct.create()
                .put(Fixture.fieldA, Fixture.bValue);

        RecordsIterable<TypedStruct> res = filter.apply(context, typedStruct, false);
        assertEquals(1, res.size());

        TypedStruct record = res.last();

        assertTrue(record.has(Fixture.fieldA));
        assertEquals(Fixture.defaultValue, record.get(Fixture.fieldA).getString());
    }

    @Test
    void when_record_match_apply_should_return_record_with_extracted_value() {
        FilterContext context = mock(FilterContext.class);
        ExtractValueFilter filter = new ExtractValueFilter();
        filter.configure(Map.of(
                REGEX_FIELD_CONFIG, Fixture.fieldA,
                REGEX_CONFIG, "^\\D*(\\d+)\\D*$",
                REGEX_DEFAULT_VALUE_CONFIG, Fixture.defaultValue));

        TypedStruct typedStruct = TypedStruct.create()
                .put(Fixture.fieldA, Fixture.fieldABValue);

        RecordsIterable<TypedStruct> res = filter.apply(context, typedStruct, false);
        assertEquals(1, res.size());

        TypedStruct record = res.last();

        assertTrue(record.has(Fixture.fieldA));
        assertEquals("123", record.get(Fixture.fieldA).getString());
        assertFalse(record.has(Fixture.targetA));
    }

    @Test
    void when_record_match_apply_with_target_defined_should_return_record_with_extracted_value_on_target_field() {
        FilterContext context = mock(FilterContext.class);
        ExtractValueFilter filter = new ExtractValueFilter();
        filter.configure(Map.of(
                REGEX_FIELD_CONFIG, Fixture.fieldA,
                REGEX_CONFIG, "^\\D*(\\d+)\\D*$",
                REGEX_DEFAULT_VALUE_CONFIG, Fixture.defaultValue,
                EXTRACT_TARGET_CONFIG, Fixture.targetA
        ));

        TypedStruct typedStruct = TypedStruct.create()
                .put(Fixture.fieldA, Fixture.fieldABValue);

        RecordsIterable<TypedStruct> res = filter.apply(context, typedStruct, false);
        assertEquals(1, res.size());

        TypedStruct record = res.last();

        assertTrue(record.has(Fixture.fieldA));
        assertTrue(record.has(Fixture.targetA));
        assertEquals(Fixture.fieldABValue, record.get(Fixture.fieldA).getString());
        assertEquals("123", record.get(Fixture.targetA).getString());
    }

    @Test
    void when_record_match_but_no_group_can_be_extracted_apply_should_return_record_with_default_value() {
        FilterContext context = mock(FilterContext.class);
        ExtractValueFilter filter = new ExtractValueFilter();
        filter.configure(Map.of(
                REGEX_FIELD_CONFIG, Fixture.fieldA,
                REGEX_CONFIG, "^\\D*\\d+\\D*$",
                REGEX_DEFAULT_VALUE_CONFIG, Fixture.defaultValue));

        TypedStruct typedStruct = TypedStruct.create()
                .put(Fixture.fieldA, Fixture.fieldABValue);

        RecordsIterable<TypedStruct> res = filter.apply(context, typedStruct, false);
        assertEquals(1, res.size());

        TypedStruct record = res.last();

        assertTrue(record.has(Fixture.fieldA));
        assertEquals(Fixture.defaultValue, record.get(Fixture.fieldA).getString());
    }


    @Test
    void when_record_no_match_and_null_config_true_apply_should_return_field_having_null_value() {
        FilterContext context = mock(FilterContext.class);
        ExtractValueFilter filter = new ExtractValueFilter();
        filter.configure(Map.of(
                REGEX_FIELD_CONFIG, Fixture.fieldA,
                REGEX_CONFIG, "[a-z]"));

        TypedStruct typedStruct = TypedStruct.create()
                .put(Fixture.fieldA, Fixture.bValue);

        RecordsIterable<TypedStruct> res = filter.apply(context, typedStruct, false);
        assertEquals(1, res.size());

        TypedStruct record = res.last();

        assertTrue(record.has(Fixture.fieldA));
        assertNull(record.get(Fixture.fieldA).getString());
    }

    interface Fixture {
        String fieldA = "fieldA";
        String targetA = "targetA";
        String bValue = "123";
        String fieldABValue = "abc123";
        String defaultValue = "-1";

    }
}