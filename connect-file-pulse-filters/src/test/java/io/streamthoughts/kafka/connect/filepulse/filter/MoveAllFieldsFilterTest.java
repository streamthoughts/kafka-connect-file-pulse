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
package io.streamthoughts.kafka.connect.filepulse.filter;

import static io.streamthoughts.kafka.connect.filepulse.config.MoveAllFieldsFilterConfig.MOVE_EXCLUDES_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.reader.RecordsIterable;
import java.util.Map;
import org.junit.jupiter.api.Test;

class MoveAllFieldsFilterTest {

    @Test
    void when_record_null_apply_should_return_empty_iterable() {
        FilterContext context = mock(FilterContext.class);
        MoveAllFieldsFilter filter = new MoveAllFieldsFilter();
        filter.configure(Map.of());

        assertNotNull(filter.apply(context, null, false));
        assertTrue(filter.apply(context, null, false).isEmpty());
    }

    @Test
    void when_typed_struct_apply_should_move_fields() {
        FilterContext context = mock(FilterContext.class);
        MoveAllFieldsFilter filter = new MoveAllFieldsFilter();
        filter.configure(Map.of());

        RecordsIterable<TypedStruct> res = filter.apply(context, Fixture.typedStruct, false);
        assertEquals(1, res.size());
        assertEquals(1, res.last().schema().fields().size());
        assertTrue(res.last().has(Fixture.defaultPayload));

        assertEquals(3, res.last().getStruct(Fixture.defaultPayload).schema().fields().size());
        assertTrue(res.last().getStruct(Fixture.defaultPayload).has(Fixture.fieldA));
        assertTrue(res.last().getStruct(Fixture.defaultPayload).has(Fixture.fieldB));
        assertTrue(res.last().getStruct(Fixture.defaultPayload).has(Fixture.fieldC));
    }

    @Test
    void when_typed_struct_with_excludes_apply_should_move_fields_but_omit_excluded_ones() {
        FilterContext context = mock(FilterContext.class);
        MoveAllFieldsFilter filter = new MoveAllFieldsFilter();
        filter.configure(Map.of(MOVE_EXCLUDES_CONFIG, Fixture.excludedFields));

        RecordsIterable<TypedStruct> res = filter.apply(context, Fixture.typedStruct, false);
        assertEquals(1, res.size());
        assertEquals(3, res.last().schema().fields().size());
        assertTrue(res.last().has(Fixture.defaultPayload));

        assertEquals(1, res.last().getStruct(Fixture.defaultPayload).schema().fields().size());
        assertFalse(res.last().getStruct(Fixture.defaultPayload).has(Fixture.fieldA));
        assertTrue(res.last().getStruct(Fixture.defaultPayload).has(Fixture.fieldB));
        assertFalse(res.last().getStruct(Fixture.defaultPayload).has(Fixture.fieldC));
    }

    interface Fixture {
        String defaultPayload = "payload";

        String fieldA = "fieldA";
        String fieldB = "fieldB";
        String fieldC = "fieldC";
        TypedStruct typedStruct = TypedStruct.create()
                .put(fieldA, "2023-03-10 14:12")
                .put(fieldB, "212")
                .put(fieldC, "2");

        String excludedFields = "fieldA,fieldC";
    }
}