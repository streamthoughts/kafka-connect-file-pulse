/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.data;

import static io.streamthoughts.kafka.connect.filepulse.data.TypedStruct.*;
import static org.junit.Assert.*;

import org.junit.Assert;
import org.junit.Test;

public class TypedStructTest {

    private static final String STRING_FIELD_1 = "string-field-1";
    private static final String STRING_FIELD_2 = "string-field-2";
    private static final String STRING_FIELD_3 = "string-field-3";
    private static final String STRING_FIELD_4 = "string-field-4";

    private static final String STRING_VALUE_1 = "string-value-1";
    private static final String STRING_VALUE_2 = "string-value-2";
    private static final String STRING_VALUE_3 = "string-value-3";
    private static final String STRING_VALUE_4 = "string-value-4";

    @Test(expected = DataException.class)
    public void should_throw_exception_given_invalid_field_name() {
        TypedStruct struct = create();
        struct.get(STRING_FIELD_1);
    }

    @Test
    public void should_return_field_previously_added() {
        TypedStruct struct = create()
                .put(STRING_FIELD_1, STRING_VALUE_1);

        TypedValue typed = struct.get(STRING_FIELD_1);
        assertNotNull(typed);
        assertEquals(Schema.string(), typed.schema());
        assertEquals(STRING_VALUE_1, typed.value());
    }

    @Test
    public void should_increment_index_while_putting_new_fields() {
        TypedStruct struct = create()
                .put(STRING_FIELD_1, STRING_VALUE_1)
                .put(STRING_FIELD_2, STRING_VALUE_2);

        assertEquals(0, struct.field(STRING_FIELD_1).index());
        assertEquals(1, struct.field(STRING_FIELD_2).index());
    }

    @Test
    public void should_remove_and_reindex_fields_given_valid_fieldname() {
        final TypedStruct struct = create()
            .put(STRING_FIELD_1, STRING_VALUE_1)
            .put(STRING_FIELD_2, STRING_FIELD_2)
            .put(STRING_FIELD_3, STRING_VALUE_3)
            .put(STRING_FIELD_4, STRING_VALUE_4);

        struct.remove(STRING_FIELD_1);
        struct.remove(STRING_FIELD_3);

        assertFalse(struct.has(STRING_FIELD_1));
        assertFalse(struct.has(STRING_FIELD_3));
        assertEquals(0, struct.field(STRING_FIELD_2).index());
        assertEquals(1, struct.field(STRING_FIELD_4).index());
    }

    @Test
    public void should_remove_and_drop_empty_struct_fields_given_valid_fieldname() {
        final TypedStruct struct = create()
            .insert("field1.child1.child2", "?")
            .insert("field2", "?");

        struct.remove("field1.child1.child2");

        assertFalse(struct.exists("field1.child1.child2"));
        assertFalse(struct.exists("field1.child1"));
        assertFalse(struct.exists("field1"));
        assertTrue(struct.exists("field2"));
    }

    @Test
    public void should_rename_given_valid_field_name() {
        final TypedStruct struct = create()
                .put(STRING_FIELD_1, STRING_VALUE_1);

        struct.rename(STRING_FIELD_1, STRING_FIELD_2);

        assertFalse(struct.has(STRING_FIELD_1));
        assertTrue(struct.has(STRING_FIELD_2));
    }

    @Test
    public void should_return_value_when_using_find_given_valid_path() {
        TypedStruct struct = create().put("foo", create().put("bar", "value"));
        Assert.assertEquals("value", struct.find("foo.bar").getString());
    }

    @Test
    public void should_return_null_when_using_find_given_invalid_path() {
        TypedStruct struct = create().put("foo", create().put("bar", "value"));
        Assert.assertNull(struct.find("foo.foo"));
    }

    @Test
    public void should_insert_value_given_valid_path() {
        TypedStruct struct = create()
                .insert("first.child", "v1")
                .insert("foo", "v2");

        Assert.assertEquals("v1", struct.getStruct("first").getString("child"));
        Assert.assertEquals("v2", struct.getString("foo"));
    }
}