/*
 * Copyright 2019 StreamThoughts.
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
package io.streamthoughts.kafka.connect.filepulse.internal;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.*;

public class SchemaUtilsTest {

    private static final String DEFAULT_FIELD_ONE = "a";
    private static final String DEFAULT_FIELD_TWO = "b";

    @Test
    public void shouldMergeSchemaGivenTwoFieldsWithDifferentName() {
        Schema schemaLeft = SchemaBuilder.struct()
                .field(DEFAULT_FIELD_ONE, Schema.STRING_SCHEMA)
                .build();
        Schema schemaRight = SchemaBuilder.struct()
                .field(DEFAULT_FIELD_TWO, Schema.STRING_SCHEMA)
                .build();

        SchemaBuilder builder = SchemaBuilder.struct();
        SchemaUtils.merge(schemaLeft, schemaRight, builder, Collections.emptySet());

        Schema schema = builder.build();

        assertNotNull(schema.field(DEFAULT_FIELD_ONE));
        assertEquals(Schema.Type.STRING, schema.field(DEFAULT_FIELD_ONE).schema().type());
        assertNotNull(schema.field(DEFAULT_FIELD_TWO));
        assertEquals(Schema.Type.STRING, schema.field(DEFAULT_FIELD_TWO).schema().type());
    }

    @Test
    public void shouldMergeSchemasGivenTwoFieldsWithDifferentTypeGivenOverride() {
        Schema schemaLeft = SchemaBuilder.struct()
                .field(DEFAULT_FIELD_ONE, Schema.STRING_SCHEMA)
                .build();


        Schema schemaRight = SchemaBuilder.struct()
                .field(DEFAULT_FIELD_ONE, Schema.INT64_SCHEMA)
                .build();

        SchemaBuilder builder = SchemaBuilder.struct();
        SchemaUtils.merge(schemaLeft, schemaRight, builder, Collections.singleton(DEFAULT_FIELD_ONE));

        Schema schema = builder.build();

        assertNotNull(schema.field(DEFAULT_FIELD_ONE));
        assertEquals(Schema.Type.INT64, schema.field(DEFAULT_FIELD_ONE).schema().type());
    }

    @Test
    public void shouldMergeSchemasGivenTwoFieldsWithSameNameIntoArray() {
        Schema schemaLeft = SchemaBuilder.struct()
                .field(DEFAULT_FIELD_ONE, Schema.STRING_SCHEMA)
                .build();

        Schema schemaRight = SchemaBuilder.struct()
                .field(DEFAULT_FIELD_ONE, Schema.STRING_SCHEMA)
                .build();

        SchemaBuilder builder = SchemaBuilder.struct();
        SchemaUtils.merge(schemaLeft, schemaRight, builder, Collections.emptySet());

        Schema schema = builder.build();

        assertNotNull(schema.field(DEFAULT_FIELD_ONE));
        assertEquals(Schema.Type.ARRAY, schema.field(DEFAULT_FIELD_ONE).schema().type());
    }

    @Test
    public void shouldMergeSchemasGivenLeftFieldWithArrayTypeEqualToRightField() {
        Schema schemaLeft = SchemaBuilder.struct()
                .field(DEFAULT_FIELD_ONE, SchemaBuilder.array(SchemaBuilder.string()))
                .build();

        Schema schemaRight = SchemaBuilder.struct()
                .field(DEFAULT_FIELD_ONE, Schema.STRING_SCHEMA)
                .build();

        SchemaBuilder builder = SchemaBuilder.struct();
        SchemaUtils.merge(schemaLeft, schemaRight, builder, Collections.emptySet());

        Schema schema = builder.build();

        assertNotNull(schema.field(DEFAULT_FIELD_ONE));
        assertEquals(Schema.Type.ARRAY, schema.field(DEFAULT_FIELD_ONE).schema().type());
    }

    @Test
    public void shouldMergeSchemasGivenRightFieldWithArrayTypeEqualToLeftField() {
        Schema schemaLeft = SchemaBuilder.struct()
                .field(DEFAULT_FIELD_ONE, Schema.STRING_SCHEMA)
                .build();

        Schema schemaRight = SchemaBuilder.struct()
                .field(DEFAULT_FIELD_ONE, SchemaBuilder.array(SchemaBuilder.string()))
                .build();

        SchemaBuilder builder = SchemaBuilder.struct();
        SchemaUtils.merge(schemaLeft, schemaRight, builder, Collections.emptySet());

        Schema schema = builder.build();

        assertNotNull(schema.field(DEFAULT_FIELD_ONE));
        assertEquals(Schema.Type.ARRAY, schema.field(DEFAULT_FIELD_ONE).schema().type());
    }

    @Test
    public void shouldMergeSchemasGivenTwoArrayFieldsWithEqualsValueType() {
        Schema schemaLeft = SchemaBuilder.struct()
                .field(DEFAULT_FIELD_ONE, SchemaBuilder.array(SchemaBuilder.string()))
                .build();

        Schema schemaRight = SchemaBuilder.struct()
                .field(DEFAULT_FIELD_ONE, SchemaBuilder.array(SchemaBuilder.string()))
                .build();

        SchemaBuilder builder = SchemaBuilder.struct();
        SchemaUtils.merge(schemaLeft, schemaRight, builder, Collections.emptySet());

        Schema schema = builder.build();

        assertNotNull(schema.field(DEFAULT_FIELD_ONE));
        assertEquals(Schema.Type.ARRAY, schema.field(DEFAULT_FIELD_ONE).schema().type());
    }

    @Test
    public void shouldThrowExceptionGivenTwoSchemasWithSameFieldButDifferentType() {
        Schema schemaLeft = SchemaBuilder.struct()
                .field(DEFAULT_FIELD_ONE, Schema.STRING_SCHEMA)
                .build();


        Schema schemaRight = SchemaBuilder.struct()
                .field(DEFAULT_FIELD_ONE, Schema.INT64_SCHEMA)
                .build();

        SchemaBuilder builder = SchemaBuilder.struct();
        try {
            SchemaUtils.merge(schemaLeft, schemaRight, builder, Collections.emptySet());
            fail("must throw an exception");
        } catch (Exception e) {
            Assert.assertEquals("Cannot merge fields 'a' with different types : STRING<>INT64", e.getMessage());
        }
    }

    @Test
    public void shouldThrowExceptionGivenLeftFieldWithArrayTypeNotEqualToRightField() {
        Schema schemaLeft = SchemaBuilder.struct()
                .field(DEFAULT_FIELD_ONE, SchemaBuilder.array(SchemaBuilder.string()))
                .build();


        Schema schemaRight = SchemaBuilder.struct()
                .field(DEFAULT_FIELD_ONE, Schema.INT64_SCHEMA)
                .build();

        SchemaBuilder builder = SchemaBuilder.struct();
        try {
            SchemaUtils.merge(schemaLeft, schemaRight, builder, Collections.emptySet());
            fail("must throw an exception");
        } catch (Exception e) {
            Assert.assertEquals("Cannot merge fields 'a' with different array value types : Array[STRING]<>INT64", e.getMessage());
        }
    }

    @Test
    public void shouldThrowExceptionGivenRightFieldWithArrayTypeNotEqualToLeftField() {
        Schema schemaLeft = SchemaBuilder.struct()
                .field(DEFAULT_FIELD_ONE,  Schema.INT64_SCHEMA)
                .build();


        Schema schemaRight = SchemaBuilder.struct()
                .field(DEFAULT_FIELD_ONE, SchemaBuilder.array(SchemaBuilder.string()))
                .build();

        SchemaBuilder builder = SchemaBuilder.struct();
        try {
            SchemaUtils.merge(schemaLeft, schemaRight, builder, Collections.emptySet());
            fail("must throw an exception");
        } catch (Exception e) {
            Assert.assertEquals("Cannot merge fields 'a' with different array value types : INT64<>Array[STRING]", e.getMessage());
        }
    }

    @Test
    public void shouldThrowExceptionGivenTwoArrayFieldWithDifferentType() {
        Schema schemaLeft = SchemaBuilder.struct()
                .field(DEFAULT_FIELD_ONE, SchemaBuilder.array(SchemaBuilder.int64()))
                .build();


        Schema schemaRight = SchemaBuilder.struct()
                .field(DEFAULT_FIELD_ONE, SchemaBuilder.array(SchemaBuilder.string()))
                .build();

        SchemaBuilder builder = SchemaBuilder.struct();
        try {
            SchemaUtils.merge(schemaLeft, schemaRight, builder, Collections.emptySet());
            fail("must throw an exception");
        } catch (Exception e) {
            Assert.assertEquals(
                String.format(
                    "Cannot merge fields '%s' of type array with different value types : Array[INT64]<>Array[STRING]",
                    DEFAULT_FIELD_ONE),
                    e.getMessage());
        }
    }
}