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
package io.streamthoughts.kafka.connect.filepulse.source.internal;

import io.streamthoughts.kafka.connect.filepulse.data.Schema;
import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ConnectSchemaMapperTest {

    private ConnectSchemaMapper mapper;

    @BeforeEach
    public void setUp() {
        mapper = new ConnectSchemaMapper();
    }

    @Test
    public void should_map_given_simple_typed_struct() {
        // GIVEN
        TypedStruct struct = TypedStruct.create()
                .put("field1", "value1")
                .put("field2", "value2");

        // WHEN
        SchemaAndValue schemaAndValue = struct.schema().map(mapper, struct, false);
        Assertions.assertNotNull(schemaAndValue);

        // THEN
        Struct connectStruct = (Struct)schemaAndValue.value();
        Assertions.assertEquals("value1", connectStruct.get("field1"));
        Assertions.assertEquals("value2", connectStruct.get("field2"));
    }

    @Test
    public void should_map_given_nested_typed_struct() {
        // GIVEN
        TypedStruct struct = TypedStruct.create()
                .put("field1", TypedStruct.create().put("field2", "value2"));

        // WHEN
        SchemaAndValue schemaAndValue = struct.schema().map(mapper, struct, false);

        // THEN
        Assertions.assertNotNull(schemaAndValue);

        Struct connectStruct = (Struct)schemaAndValue.value();
        Struct field1 = (Struct)connectStruct.get("field1");
        Assertions.assertNotNull(field1);

        Assertions.assertEquals("value2", field1.get("field2"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void should_map_given_type_struct_with_array_field() {
        // GIVEN
        TypedStruct struct = TypedStruct.create()
                .put("field1", Collections.singletonList("value"));

        // WHEN
        SchemaAndValue schemaAndValue = struct.schema().map(mapper, struct, false);

        // THEN
        Assertions.assertNotNull(schemaAndValue);

        Struct connectStruct = (Struct)schemaAndValue.value();
        List<String> field1 = (List<String>)connectStruct.get("field1");
        Assertions.assertNotNull(field1);
        Assertions.assertEquals("value", field1.get(0));
    }

    @Test
    @SuppressWarnings("unchecked")
    void should_map_given_type_struct_with_array_of_struct() {
        // GIVEN
        TypedStruct struct = TypedStruct.create()
                .put("field1", Collections.singletonList(TypedStruct.create().put("field2", "value")));

        // WHEN
        SchemaAndValue schemaAndValue = struct.schema().map(mapper, struct, false);

        // THEN
        Assertions.assertNotNull(schemaAndValue);

        Struct connectStruct = (Struct)schemaAndValue.value();
        List<Struct> field1 = (List<Struct>)connectStruct.get("field1");
        Assertions.assertNotNull(field1);
        Assertions.assertEquals("value", field1.get(0).getString("field2"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void should_map_given_type_struct_with_map_field() {
        // GIVEN
        TypedStruct struct = TypedStruct.create()
                .put("field1", Collections.singletonMap("field2", "value"));

        // WHEN
        SchemaAndValue schemaAndValue = struct.schema().map(mapper, struct, false);

        // THEN
        Assertions.assertNotNull(schemaAndValue);

        Struct connectStruct = (Struct)schemaAndValue.value();
        Map<String, String> field1 = (Map<String, String>)connectStruct.get("field1");
        Assertions.assertNotNull(field1);
        Assertions.assertEquals("value", field1.get("field2"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void should_map_given_type_struct_with_map_with_struct_value() {
        // GIVEN
        TypedStruct struct = TypedStruct.create()
            .put("field1",
                Collections.singletonMap(
                    "field2",
                    TypedStruct.create().put("field3", "value")
                ));

        // WHEN
        SchemaAndValue schemaAndValue = struct.schema().map(mapper, struct, false);

        // THEN
        Assertions.assertNotNull(schemaAndValue);
        Struct connectStruct = (Struct)schemaAndValue.value();
        Map<String, Struct> field1 = (Map<String, Struct>)connectStruct.get("field1");
        Assertions.assertNotNull(field1);
        Assertions.assertEquals("value", field1.get("field2").getString("field3"));
    }

    @Test
    void should_map_given_type_struct_with_null_value() {
        // GIVEN
        TypedStruct struct = TypedStruct.create()
                .put("field1", "value1")
                .put("field2", Schema.none(), null);

        // WHEN
        SchemaAndValue schemaAndValue = struct.schema().map(mapper, struct, false);

        // THEN
        Assertions.assertNotNull(schemaAndValue);
        Struct connectStruct = (Struct) schemaAndValue.value();
        Assertions.assertNotNull(connectStruct.schema().field("field1"));
        Assertions.assertNull(connectStruct.schema().field("field2"));
    }

    @Test
    void should_map_given_type_struct_with_empty_array() {
        // GIVEN
        TypedStruct struct = TypedStruct.create()
                .put("field1", "value1")
                .put("field2", Schema.array(Collections.emptyList(), null), Collections.emptyList());

        // WHEN
        SchemaAndValue schemaAndValue = struct.schema().map(mapper, struct, false);

        // THEN
        Assertions.assertNotNull(schemaAndValue);
        Struct connectStruct = (Struct)schemaAndValue.value();
        Assertions.assertNotNull(connectStruct.schema().field("field1"));
        Assertions.assertNull(connectStruct.schema().field("field2"));
    }

    @Test
    void test_normalize_schema_name_given_leading_underscore_false() {
        // GIVEN
        var mapper = new ConnectSchemaMapper();

        // WHEN - THEN
        Assertions.assertEquals("Foo", mapper.normalizeSchemaName("foo"));
        Assertions.assertEquals("FooBar", mapper.normalizeSchemaName("foo_bar"));
        Assertions.assertEquals("FooBar", mapper.normalizeSchemaName("foo.bar"));
        Assertions.assertEquals("FooBar", mapper.normalizeSchemaName("__foo_bar"));
    }

    @Test
    void test_normalize_schema_name_given_leading_underscore_true() {
        // GIVEN
        var mapper = new ConnectSchemaMapper();
        mapper.setKeepLeadingUnderscoreCharacters(true);

        // WHEN - THEN
        Assertions.assertEquals("__FooBar", mapper.normalizeSchemaName("__foo_bar"));
    }

    @Test
    void should_map_given_struct_with_duplicate_schema() {
        // GIVEN
        ConnectSchemaMapper mapper = new ConnectSchemaMapper();
        final TypedStruct struct = TypedStruct.create()
                .put("field1", TypedStruct.create("Foo")
                        .put("field1", "val")
                        .put("field2", "val"))
                .put("field2", TypedStruct.create("Foo")
                        .put("field2", "val")
                        .put("field3", "val")
                );

        // WHEN
        final org.apache.kafka.connect.data.Schema connectSchema = mapper.map(struct.schema(), false);

        // THEN
        Assertions.assertEquals(
                connectSchema.field("field1").schema(),
                connectSchema.field("field2").schema()
        );
    }

    @Test
    void should_map_struct_with_map_containing_different_type_value() {
        // GIVEN
        final TypedStruct struct = TypedStruct.create()
                .put("map", Map.of("k1", "string", "k2", 0L));

        // WHEN
        SchemaAndValue schemaAndValue = struct.schema().map(mapper, struct, false);

        // THEN
        Assertions.assertNotNull(schemaAndValue);

        org.apache.kafka.connect.data.Schema mapSchema = schemaAndValue.schema().field("map").schema();
        Assertions.assertEquals(org.apache.kafka.connect.data.Schema.Type.MAP, mapSchema.schema().type());
        Assertions.assertEquals(org.apache.kafka.connect.data.Schema.Type.STRING, mapSchema.schema().valueSchema().type());
    }

    @Test
    void should_map_struct_with_array_containing_different_type_value() {
        // GIVEN
        final TypedStruct struct = TypedStruct
                .create()
                .put("array", List.of("string", 0L));

        // WHEN
        SchemaAndValue schemaAndValue = struct.schema().map(mapper, struct, false);

        // THEN
        Assertions.assertNotNull(schemaAndValue);
        org.apache.kafka.connect.data.Schema mapSchema = schemaAndValue.schema().field("array").schema();
        Assertions.assertEquals(org.apache.kafka.connect.data.Schema.Type.ARRAY, mapSchema.schema().type());
        Assertions.assertEquals(org.apache.kafka.connect.data.Schema.Type.STRING, mapSchema.schema().valueSchema().type());
    }
}
