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

import java.util.function.Function;
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

        Function<Schema, Void> assertions = schema -> {
            Assert.assertNotNull(schema);
            Assert.assertEquals(1, ((StructSchema)schema).fields().size());
            Assert.assertEquals(Type.STRUCT, ((StructSchema)schema).field("field-1").type());

            final StructSchema nested = (StructSchema)((StructSchema) schema).field("field-1").schema();
            Assert.assertEquals(2, nested.fields().size());
            Assert.assertEquals(Type.STRING, nested.field("field-2").type());
            Assert.assertEquals(Type.INTEGER, nested.field("field-3").type());

            return null;
        };

        assertions.apply(schema1.merge(schema2));
        assertions.apply(schema2.merge(schema1));
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

        Function<Schema, Void> assertions = schema -> {
            Assert.assertNotNull(schema);
            Assert.assertEquals(1, ((StructSchema)schema).fields().size());
            Assert.assertEquals(Type.ARRAY, ((StructSchema)schema).field("field-1").type());
            final StructSchema nested = (StructSchema)((ArraySchema)((StructSchema) schema).field("field-1").schema()).valueSchema();
            Assert.assertEquals(2, nested.fields().size());
            Assert.assertEquals(Type.STRING, nested.field("field-2").type());
            Assert.assertEquals(Type.INTEGER, nested.field("field-3").type());
            return null;
        };

        assertions.apply(schema1.merge(schema2));
        assertions.apply(schema2.merge(schema1));
    }

    @Test
    public void should_success_merge_given_two_schemas_with_array_primitive() {
        final StructSchema schema1 = new StructSchema()
                .name("test")
                .namespace("namespace")
                .field("field-1", Schema.string());

        final StructSchema schema2 = new StructSchema()
                .name("test")
                .namespace("namespace")
                .field("field-1", Schema.array(Schema.string()));

        Function<Schema, Void> assertions = schema -> {
            Assert.assertNotNull(schema);
            Assert.assertEquals(Type.ARRAY, ((StructSchema) schema).field("field-1").type());
            Assert.assertEquals(Type.STRING, ((ArraySchema) ((StructSchema) schema).field("field-1").schema()).valueSchema().type());
            return null;
        };

        assertions.apply(schema1.merge(schema2));
        assertions.apply(schema2.merge(schema1));
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

        Function<Schema, Void> assertions = schema -> {
            Assert.assertNotNull(schema);
            Assert.assertEquals(Type.STRUCT, schema.type());
            Assert.assertEquals(2, ((StructSchema)schema).fields().size());
            Assert.assertEquals(Type.STRING, ((StructSchema)schema).field("field-one").type());
            Assert.assertEquals(Type.INTEGER, ((StructSchema)schema).field("field-two").type());
            return null;
        };

        assertions.apply(schema1.merge(schema2));
        assertions.apply(schema2.merge(schema1));
    }

    @Test(expected = DataException.class)
    public void should_failed_merged_given_two_schemas_with_incompatible_fields() {
        final StructSchema schema1 = new StructSchema()
                .name("test")
                .namespace("namespace")
                .field("field-one", Schema.float32());

        final StructSchema schema2 = new StructSchema()
                .name("test")
                .namespace("namespace")
                .field("field-one",  Schema.bool());

        schema1.merge(schema2); // expecting failure
    }

    @Test
    public void should_failed_merged_given_two_schemas_with_fields_compatible_to_long() {
        final StructSchema schema1 = new StructSchema()
                .name("test")
                .namespace("namespace")
                .field("field-one", Schema.int32());

        final StructSchema schema2 = new StructSchema()
                .name("test")
                .namespace("namespace")
                .field("field-one",  Schema.int64());

        final Schema merged = schema1.merge(schema2);
        Assert.assertNotNull(merged);
        Assert.assertEquals(Type.STRUCT, merged.type());
        Assert.assertEquals(1, ((StructSchema)merged).fields().size());
        Assert.assertEquals(Type.LONG, ((StructSchema)merged).field("field-one").type());
    }

    @Test
    public void should_failed_merged_given_two_schemas_with_fields_compatible_to_double() {
        final StructSchema schema1 = new StructSchema()
                .name("test")
                .namespace("namespace")
                .field("field-one", Schema.int32());

        final StructSchema schema2 = new StructSchema()
                .name("test")
                .namespace("namespace")
                .field("field-one",  Schema.float64());

        final Schema merged = schema1.merge(schema2);
        Assert.assertNotNull(merged);
        Assert.assertEquals(Type.STRUCT, merged.type());
        Assert.assertEquals(1, ((StructSchema)merged).fields().size());
        Assert.assertEquals(Type.DOUBLE, ((StructSchema)merged).field("field-one").type());
    }
}