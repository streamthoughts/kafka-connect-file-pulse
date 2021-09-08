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
package io.streamthoughts.kafka.connect.filepulse.internal;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Assert;
import org.junit.Test;

import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class SchemaUtilsTest {

    private static final String DEFAULT_FIELD_A = "A";
    private static final String DEFAULT_FIELD_B = "B";

    @Test
    public void should_success_merge_given_two_identical_primitive_schemas() {
        Schema schemaLeft = SchemaBuilder.struct()
                .field(DEFAULT_FIELD_A, Schema.STRING_SCHEMA)
                .build();

        Schema schemaRight = SchemaBuilder.struct()
                .field(DEFAULT_FIELD_A, Schema.STRING_SCHEMA)
                .build();

        Schema schema = SchemaUtils.merge(schemaLeft, schemaRight);

        assertNotNull(schema.field(DEFAULT_FIELD_A));
        assertEquals(Schema.STRING_SCHEMA, schema.field(DEFAULT_FIELD_A).schema());
    }

    @Test
    public void should_success_merge_given_schema_with_two_distinct_field_name() {
        Schema schemaLeft = SchemaBuilder.struct()
                .field(DEFAULT_FIELD_A, Schema.STRING_SCHEMA)
                .build();

        Schema schemaRight = SchemaBuilder.struct()
                .field(DEFAULT_FIELD_B, Schema.STRING_SCHEMA)
                .build();

        Schema schema = SchemaUtils.merge(schemaLeft, schemaRight);

        assertNotNull(schema.field(DEFAULT_FIELD_A));
        assertEquals(Schema.Type.STRING, schema.field(DEFAULT_FIELD_A).schema().type());

        assertNotNull(schema.field(DEFAULT_FIELD_B));
        assertEquals(Schema.Type.STRING, schema.field(DEFAULT_FIELD_B).schema().type());
    }

    @Test
    public void should_success_merge_given_schemas_with_two_distinct_field_type() {
        Schema schemaLeft = SchemaBuilder.struct()
                .field(DEFAULT_FIELD_A, Schema.STRING_SCHEMA)
                .build();


        Schema schemaRight = SchemaBuilder.struct()
                .field(DEFAULT_FIELD_A, Schema.INT64_SCHEMA)
                .build();

        Schema schema = SchemaUtils.merge(schemaLeft, schemaRight);

        assertNotNull(schema.field(DEFAULT_FIELD_A));
        assertEquals(Schema.Type.STRING, schema.field(DEFAULT_FIELD_A).schema().type());
    }

    @Test
    public void should_success_merge_given_array_schema_and_primitive_schema() {
        Schema schemaLeft = SchemaBuilder.struct()
                .field(DEFAULT_FIELD_A, SchemaBuilder.array(SchemaBuilder.string()))
                .build();

        Schema schemaRight = SchemaBuilder.struct()
                .field(DEFAULT_FIELD_A, Schema.STRING_SCHEMA)
                .build();

        Function<Schema, Void> assertions = s -> {
            assertNotNull(s.field(DEFAULT_FIELD_A));
            assertEquals(Schema.Type.ARRAY, s.field(DEFAULT_FIELD_A).schema().type());
            return null;
        };

        assertions.apply(SchemaUtils.merge(schemaLeft, schemaRight));
        assertions.apply(SchemaUtils.merge(schemaRight, schemaLeft));
    }

    @Test
    public void should_success_merge_given_two_identical_array_schemas() {
        Schema schemaLeft = SchemaBuilder.struct()
                .field(DEFAULT_FIELD_A, SchemaBuilder.array(SchemaBuilder.string()))
                .build();

        Schema schemaRight = SchemaBuilder.struct()
                .field(DEFAULT_FIELD_A, SchemaBuilder.array(SchemaBuilder.string()))
                .build();

        Schema schema = SchemaUtils.merge(schemaLeft, schemaRight);

        assertNotNull(schema.field(DEFAULT_FIELD_A));
        assertEquals(Schema.Type.ARRAY, schema.field(DEFAULT_FIELD_A).schema().type());
    }
}