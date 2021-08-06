/*
 * Copyright 2021 StreamThoughts.
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

import org.junit.Assert;
import org.junit.Test;

public class StructSchemaTest {

    @Test
    public void should_success_merge_given_two_identical_schemas() {
        final StructSchema schema = new StructSchema()
                .name("test")
                .namespace("namespace")
                .field("field-1", Schema.string())
                .field("field-2", Schema.int32());

        final Schema merged = schema.merge(schema);
        Assert.assertNotNull(merged);
        Assert.assertEquals(merged, schema);
        Assert.assertEquals(2, ((StructSchema)merged).fields().size());
        Assert.assertEquals(Type.STRING, ((StructSchema)merged).field("field-1").type());
        Assert.assertEquals(Type.INTEGER, ((StructSchema)merged).field("field-2").type());
    }

    @Test
    public void should_success_merge_given_two_schemas_with_nested_structs() {
        final StructSchema schema1 = new StructSchema()
                .name("test")
                .namespace("namespace")
                .field("field-1", new StructSchema().field("field-2", Schema.string()));

        final StructSchema schema2 = new StructSchema()
                .name("test")
                .namespace("namespace")
                .field("field-1", new StructSchema().field("field-3", Schema.int32()));

        final Schema merged = schema1.merge(schema2);
        Assert.assertNotNull(merged);
        Assert.assertEquals(1, ((StructSchema)merged).fields().size());
        Assert.assertEquals(Type.STRUCT, ((StructSchema)merged).field("field-1").type());
        final StructSchema nested = (StructSchema)((StructSchema) merged).field("field-1").schema();
        Assert.assertEquals(2, nested.fields().size());
        Assert.assertEquals(Type.STRING, nested.field("field-2").type());
        Assert.assertEquals(Type.INTEGER, nested.field("field-3").type());
    }

    @Test
    public void should_success_merge_given_two_schemas_with_array_structs() {
        final StructSchema schema1 = new StructSchema()
                .name("test")
                .namespace("namespace")
                .field("field-1", Schema.array(new StructSchema().field("field-2", Schema.string())));

        final StructSchema schema2 = new StructSchema()
                .name("test")
                .namespace("namespace")
                .field("field-1", new StructSchema().field("field-3", Schema.int32()));

        final Schema merged = schema1.merge(schema2);
        Assert.assertNotNull(merged);
        Assert.assertEquals(1, ((StructSchema)merged).fields().size());
        Assert.assertEquals(Type.ARRAY, ((StructSchema)merged).field("field-1").type());
        final StructSchema nested = (StructSchema)((ArraySchema)((StructSchema) merged).field("field-1").schema()).valueSchema();
        Assert.assertEquals(2, nested.fields().size());
        Assert.assertEquals(Type.STRING, nested.field("field-2").type());
        Assert.assertEquals(Type.INTEGER, nested.field("field-3").type());
    }

    @Test
    public void should_success_merge_given_two_schemas_with_additional_fields() {
        final StructSchema schema1 = new StructSchema()
                .name("test")
                .namespace("namespace")
                .field("field-one", Schema.string());

        final StructSchema schema2 = new StructSchema()
                .name("test")
                .namespace("namespace")
                .field("field-two", Schema.int32());

        final Schema merged = schema1.merge(schema2);
        Assert.assertNotNull(merged);
        Assert.assertEquals(Type.STRUCT, merged.type());
        Assert.assertEquals(2, ((StructSchema)merged).fields().size());
        Assert.assertEquals(Type.STRING, ((StructSchema)merged).field("field-one").type());
        Assert.assertEquals(Type.INTEGER, ((StructSchema)merged).field("field-two").type());
    }


    @Test(expected = DataException.class)
    public void should_failed_merged_given_two_schemas_with_incompatible_fields() {
        final StructSchema schema1 = new StructSchema()
                .name("test")
                .namespace("namespace")
                .field("field-one", Schema.string());

        final StructSchema schema2 = new StructSchema()
                .name("test")
                .namespace("namespace")
                .field("field-one", Schema.int32());

        schema1.merge(schema2); // expecting failure
    }
}