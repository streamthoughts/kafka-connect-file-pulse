/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.data;

import java.util.List;
import org.junit.Assert;
import org.junit.Test;

public class LazyArraySchemaTest {

    @Test
    public void should_merge_schema_given_array_with_multiple_items() {

        final TypedStruct o1 = TypedStruct.create().put("f1", "????");
        final TypedStruct o2 = TypedStruct.create().put("f2", "????");
        final ArraySchema schema = Schema.array(List.of(o1, o2), null);

        final Schema valueSchema = schema.valueSchema();
        Assert.assertEquals(Type.STRUCT, valueSchema.type());

        final StructSchema structSchema = (StructSchema) valueSchema;
        Assert.assertEquals(2, structSchema.fields().size());
        Assert.assertNotNull(structSchema.field("f1"));
        Assert.assertNotNull(structSchema.field("f2"));
    }

    @Test
    public void should_merge_schemas_given_empty_left_array() {
        LazyArraySchema s1 = new LazyArraySchema(List.of("foo"));
        LazyArraySchema s2 = new LazyArraySchema(List.of());
        Schema merged = s2.merge(s1);

        Assert.assertEquals(Type.ARRAY, merged.type());
        Assert.assertEquals(Type.STRING, ((ArraySchema)merged).valueSchema().type());
    }

    @Test
    public void should_merge_schemas_given_empty_right_array() {
        LazyArraySchema s1 = new LazyArraySchema(List.of("foo"));
        LazyArraySchema s2 = new LazyArraySchema(List.of());
        Schema merged = s1.merge(s2);

        Assert.assertEquals(Type.ARRAY, merged.type());
        Assert.assertEquals(Type.STRING, ((ArraySchema)merged).valueSchema().type());
    }
}