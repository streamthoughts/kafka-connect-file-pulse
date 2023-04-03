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