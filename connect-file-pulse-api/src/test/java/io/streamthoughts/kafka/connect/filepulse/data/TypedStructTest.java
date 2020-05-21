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
package io.streamthoughts.kafka.connect.filepulse.data;

import org.junit.Test;

import static org.junit.Assert.*;

public class TypedStructTest {

    private static final String STRING_FIELD_1 = "string-field-1";
    private static final String STRING_FIELD_2 = "string-field-2";
    private static final String STRING_FIELD_3 = "string-field-3";

    private static final String STRING_VALUE_1 = "string-value-1";
    private static final String STRING_VALUE_2 = "string-value-2";
    private static final String STRING_VALUE_3 = "string-value-3";

    @Test(expected = DataException.class)
    public void shouldThrowExceptionGivenInvalidFieldName() {
        TypedStruct struct = TypedStruct.create();
        struct.get(STRING_FIELD_1);
    }

    @Test
    public void shouldReturnFieldPreviouslyAdded() {
        TypedStruct struct = TypedStruct.create()
                .put(STRING_FIELD_1, STRING_VALUE_1);

        TypedValue typed = struct.get(STRING_FIELD_1);
        assertNotNull(typed);
        assertEquals(Schema.string(), typed.schema());
        assertEquals(STRING_VALUE_1, typed.value());
    }

    @Test
    public void shouldIncrementIndexWhilePuttingNewFields() {
        TypedStruct struct = TypedStruct.create()
                .put(STRING_FIELD_1, STRING_VALUE_1)
                .put(STRING_FIELD_2, STRING_VALUE_2);

        assertEquals(0, struct.field(STRING_FIELD_1).index());
        assertEquals(1, struct.field(STRING_FIELD_2).index());
    }

    @Test
    public void shouldRemoveAndReIndexFieldsGivenValidFieldName() {
        final TypedStruct struct = TypedStruct.create()
                .put(STRING_FIELD_1, STRING_VALUE_1)
                .put(STRING_FIELD_2, STRING_FIELD_2)
                .put(STRING_FIELD_3, STRING_VALUE_3);

        struct.remove(STRING_FIELD_2);

        assertFalse(struct.has(STRING_FIELD_2));
        assertEquals(0, struct.field(STRING_FIELD_1).index());
        assertEquals(1, struct.field(STRING_FIELD_3).index());
    }

    @Test
    public void shouldRenameGivenValidFieldName() {
        final TypedStruct struct = TypedStruct.create()
                .put(STRING_FIELD_1, STRING_VALUE_1);

        struct.rename(STRING_FIELD_1, STRING_FIELD_2);

        assertFalse(struct.has(STRING_FIELD_1));
        assertTrue(struct.has(STRING_FIELD_2));
    }
}