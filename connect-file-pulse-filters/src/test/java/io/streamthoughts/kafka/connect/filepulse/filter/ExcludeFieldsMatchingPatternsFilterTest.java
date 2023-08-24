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

import static io.streamthoughts.kafka.connect.filepulse.config.ExcludeFieldsMatchingPatternsConfig.EXCLUDE_FIELDS_BLOCK_FIELD_CONFIG;
import static io.streamthoughts.kafka.connect.filepulse.config.ExcludeFieldsMatchingPatternsConfig.EXCLUDE_FIELDS_REGEX_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.reader.RecordsIterable;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ExcludeFieldsMatchingPatternsFilterTest {

    @Test
    void when_record_null_apply_should_return_empty_iterable() {
        FilterContext context = mock(FilterContext.class);
        ExcludeFieldsMatchingPatternsFilter filter = new ExcludeFieldsMatchingPatternsFilter();
        filter.configure(Map.of(EXCLUDE_FIELDS_REGEX_CONFIG, "[a-z]"));

        RecordsIterable<TypedStruct> iterable = filter.apply(context, null, false);
        assertNotNull(iterable);
        assertEquals(0, iterable.size());
    }

    @Test
    void when_record_apply_should_propagate_null_for_fields_matching_regex_and_propagate_all_other_fields() {
        FilterContext context = mock(FilterContext.class);
        ExcludeFieldsMatchingPatternsFilter filter = new ExcludeFieldsMatchingPatternsFilter();
        filter.configure(Map.of(EXCLUDE_FIELDS_REGEX_CONFIG, Fixture.nullFieldRegex));

        TypedStruct typedStruct = TypedStruct.create()
                .put(Fixture.fieldA, Fixture.fieldAValue)
                .put(Fixture.fieldB, Fixture.fieldBValue)
                .put(Fixture.fieldC, Fixture.fieldCValue)
                .put(Fixture.fieldD, Fixture.fieldDValue);

        RecordsIterable<TypedStruct> res = filter.apply(context, typedStruct, false);
        assertEquals(1, res.size());

        TypedStruct record = res.last();

        assertTrue(record.has(Fixture.fieldA));
        assertTrue(record.has(Fixture.fieldB));
        assertTrue(record.has(Fixture.fieldC));
        assertTrue(record.has(Fixture.fieldD));

        assertEquals(Fixture.fieldAValue, record.get(Fixture.fieldA).getString());
        assertNull(record.get(Fixture.fieldB).getString());
        assertEquals(Fixture.fieldCValue, record.get(Fixture.fieldC).getString());
        assertNull(record.get(Fixture.fieldD).getString());

    }

    @Test
    void when_record_apply_should_block_fields_matching_regex_and_propagate_all_other_fields() {
        FilterContext context = mock(FilterContext.class);
        ExcludeFieldsMatchingPatternsFilter filter = new ExcludeFieldsMatchingPatternsFilter();
        filter.configure(Map.of(EXCLUDE_FIELDS_REGEX_CONFIG, Fixture.nullFieldRegex,
                EXCLUDE_FIELDS_BLOCK_FIELD_CONFIG, "true"));

        TypedStruct typedStruct = TypedStruct.create()
                .put(Fixture.fieldA, Fixture.fieldAValue)
                .put(Fixture.fieldB, Fixture.fieldBValue)
                .put(Fixture.fieldC, Fixture.fieldCValue)
                .put(Fixture.fieldD, Fixture.fieldDValue);

        RecordsIterable<TypedStruct> res = filter.apply(context, typedStruct, false);
        assertEquals(1, res.size());

        TypedStruct record = res.last();

        assertTrue(record.has(Fixture.fieldA));
        assertFalse(record.has(Fixture.fieldB));
        assertTrue(record.has(Fixture.fieldC));
        assertFalse(record.has(Fixture.fieldD));

        assertEquals(Fixture.fieldAValue, record.get(Fixture.fieldA).getString());
        assertEquals(Fixture.fieldCValue, record.get(Fixture.fieldC).getString());
    }

    interface Fixture {
        String nullFieldRegex = "null";

        String fieldA = "fieldA";
        String fieldB = "fieldB";
        String fieldC = "fieldC";

        String fieldD = "fieldD";

        String fieldAValue = "2021-01-010 14:12";
        String fieldBValue = "null";
        String fieldCValue = "Hello";

        String fieldDValue = null;

    }
}