/*
 * Copyright 2019-2020 StreamThoughts.
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
package io.streamthoughts.kafka.connect.filepulse.data.merger;

import static org.junit.Assert.*;

import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import java.util.Collections;
import org.junit.Test;

public class TypeValueMergerTest {

    private static final String FIELD_VALUE_A = "a";
    private static final String FIELD_VALUE_B = "b";

    private static final String VALUE_A = "value-a";
    private static final String VALUE_B = "value-b";

    private final TypeValueMerger merger = new DefaultTypeValueMerger();

    @Test
    public void should_merge_struct_given_two_fields_with_different_name() {
        final TypedStruct structLeft = TypedStruct.create()
                .put(FIELD_VALUE_A, VALUE_A);

        final TypedStruct structRight = TypedStruct.create()
                .put(FIELD_VALUE_B, VALUE_B);

        final TypedStruct merged = merger.merge(structLeft, structRight, Collections.emptySet());

        assertNotNull(merged);

        assertEquals(VALUE_A, merged.getString(FIELD_VALUE_A));
        assertEquals(VALUE_B, merged.getString(FIELD_VALUE_B));
    }

    @Test
    public void should_merge_struct_given_two_fields_with_different_type_given_override() {
        final TypedStruct structLeft = TypedStruct.create()
                .put(FIELD_VALUE_A, VALUE_A);

        final TypedStruct structRight = TypedStruct.create()
                .put(FIELD_VALUE_A, VALUE_B);

        final TypedStruct merged = merger.merge(structLeft, structRight, Collections.singleton(FIELD_VALUE_A));

        assertNotNull(merged);
        assertEquals(VALUE_B,  merged.getString(FIELD_VALUE_A));
    }

    @Test
    public void should_merge_struct_given_two_fields_with_same_name_into_array() {
        final TypedStruct structLeft = TypedStruct.create()
                .put(FIELD_VALUE_A, VALUE_A);

        final TypedStruct structRight = TypedStruct.create()
                .put(FIELD_VALUE_A, VALUE_B);

        final TypedStruct merged = merger.merge(structLeft, structRight, Collections.emptySet());

        assertNotNull(merged);

        assertEquals(2, merged.getArray(FIELD_VALUE_A).size());
        assertEquals(VALUE_A,  merged.getArray(FIELD_VALUE_A).get(0));
        assertEquals(VALUE_B,  merged.getArray(FIELD_VALUE_A).get(1));
    }

    @Test
    public void should_merge_struct_given_two_identical_paths_into_array() {
        final TypedStruct structLeft = TypedStruct.create()
                .insert("a.b1", VALUE_A)
                .insert("a.b2", VALUE_A);

        final TypedStruct structRight = TypedStruct.create()
                .insert("a.b2", VALUE_B);

        final TypedStruct merged = merger.merge(structLeft, structRight, Collections.emptySet());

        assertNotNull(merged);

        assertEquals(VALUE_A,  merged.find("a.b1").getString());
        assertEquals(2,  merged.find("a.b2").getArray().size());
        assertTrue(merged.find("a.b2").getArray().contains(VALUE_A));
        assertTrue(merged.find("a.b2").getArray().contains(VALUE_B));
    }

    @Test
    public void should_merge_struct_given_two_existing_paths_and_overwrite() {
        final TypedStruct structLeft = TypedStruct.create()
                .insert("a.b1", VALUE_A)
                .insert("a.b2", VALUE_A);

        final TypedStruct structRight = TypedStruct.create()
                .insert("a.b2", VALUE_B);

        final TypedStruct merged = merger.merge(structLeft, structRight, Collections.singleton("a.b2"));

        assertNotNull(merged);

        assertEquals(VALUE_A,  merged.find("a.b1").getString());
        assertEquals(VALUE_B,  merged.find("a.b2").getString());
    }

    @Test
    public void should_merge_struct_given_two_existing_paths_into_array() {
        final TypedStruct structLeft = TypedStruct.create()
                .insert("a.b1", VALUE_A)
                .insert("a.b2", VALUE_A);

        final TypedStruct structRight = TypedStruct.create()
                .insert("a.b2", VALUE_B);

        final TypedStruct merged = merger.merge(structLeft, structRight, Collections.emptySet());

        assertNotNull(merged);

        assertEquals(VALUE_A,  merged.find("a.b1").getString());
        assertEquals(2,  merged.find("a.b2").getArray().size());
    }

    @Test
    public void should_merge_struct_given_left_field_with_array_type_equal_to_right_field() {
        final TypedStruct structLeft = TypedStruct.create()
                .put(FIELD_VALUE_A, Collections.singletonList(VALUE_A));

        final TypedStruct structRight = TypedStruct.create()
                .put(FIELD_VALUE_A, VALUE_B);

        final TypedStruct merged = merger.merge(structLeft, structRight, Collections.emptySet());

        assertNotNull(merged);

        assertEquals(2, merged.getArray(FIELD_VALUE_A).size());
        assertEquals(VALUE_A,  merged.getArray(FIELD_VALUE_A).get(0));
        assertEquals(VALUE_B,  merged.getArray(FIELD_VALUE_A).get(1));
    }

    @Test
    public void shouldMergeStructGivenRightFieldWithArrayTypeEqualToLeftField() {
        final TypedStruct structLeft = TypedStruct.create()
                .put(FIELD_VALUE_A, VALUE_A);

        final TypedStruct structRight = TypedStruct.create()
                .put(FIELD_VALUE_A, Collections.singletonList(VALUE_B));

        final TypedStruct merged = merger.merge(structLeft, structRight, Collections.emptySet());

        assertNotNull(merged);

        assertEquals(2, merged.getArray(FIELD_VALUE_A).size());
        assertEquals(VALUE_A,  merged.getArray(FIELD_VALUE_A).get(0));
        assertEquals(VALUE_B,  merged.getArray(FIELD_VALUE_A).get(1));
    }

    @Test
    public void should_merge_struct_given_two_array_fields_with_equals_value_type() {
        final TypedStruct structLeft = TypedStruct.create()
                .put(FIELD_VALUE_A, Collections.singletonList(VALUE_A));

        final TypedStruct structRight = TypedStruct.create()
                .put(FIELD_VALUE_A, Collections.singletonList(VALUE_B));

        final TypedStruct merged = merger.merge(structLeft, structRight, Collections.emptySet());

        assertNotNull(merged);

        assertEquals(2, merged.getArray(FIELD_VALUE_A).size());
        assertEquals(VALUE_A,  merged.getArray(FIELD_VALUE_A).get(0));
        assertEquals(VALUE_B,  merged.getArray(FIELD_VALUE_A).get(1));
    }

    @Test
    public void should_merged_given_two_struct_with_common_child_fields() {
        final TypedStruct structLeft = TypedStruct.create()
            .insert("a.b1.c", VALUE_A);

        final TypedStruct structRight = TypedStruct.create()
            .insert("a.b2.c", VALUE_B);

        final TypedStruct merged = merger.merge(structLeft, structRight, Collections.emptySet());

        assertNotNull(merged);

        assertEquals(VALUE_A,  merged.find("a.b1.c").getString());
        assertEquals(VALUE_B,  merged.find("a.b2.c").getString());
    }
}