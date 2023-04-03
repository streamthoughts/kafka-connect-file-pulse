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
package io.streamthoughts.kafka.connect.filepulse.json;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import io.streamthoughts.kafka.connect.filepulse.data.ArraySchema;
import io.streamthoughts.kafka.connect.filepulse.data.Schema;
import io.streamthoughts.kafka.connect.filepulse.data.StructSchema;
import io.streamthoughts.kafka.connect.filepulse.data.Type;
import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.data.TypedValue;
import java.util.Arrays;
import org.junit.Assert;
import org.junit.Test;

public class DefaultJSONStructConverterTest {

    private final JSONStructConverter converter = new DefaultJSONStructConverter();

    @Test
    public void shouldConvertGivenArrayOfComplexType() throws Exception {
        TypedValue struct = converter.readJson("[{\"firstName\": \"foo\"}, {\"firstName\": \"bar\"}]");
        Assert.assertNotNull(struct);
    }

    @Test
    public void shouldConvertGivenFieldWithArrayOfComplexType() throws Exception {
        TypedValue value = converter.readJson("{\"field-one\" : [{\"firstName\": \"foo\"}, {\"firstName\": \"bar\"}]}");

        Assert.assertNotNull(value);
        Assert.assertEquals(Type.STRUCT, value.type());
        TypedStruct struct = value.getStruct();

        StructSchema schema = struct.schema();
        assertNotNull(schema.field("field-one"));
        Schema fieldSchema = schema.field("field-one").schema();
        assertEquals(Type.ARRAY, fieldSchema.type());
        Schema arraySchema = ((ArraySchema) fieldSchema).valueSchema();
        assertEquals(Type.STRUCT, arraySchema.type());
    }

    @Test
    public void shouldConvertGivenFieldsWithStringType() throws Exception {

        TypedValue value = converter.readJson("{\"field-one\" : \"one\", \"field-two\":\"two\"}");

        Assert.assertNotNull(value);
        Assert.assertEquals(Type.STRUCT, value.type());
        TypedStruct struct = value.getStruct();

        StructSchema schema = struct.schema();
        assertEquals(2, schema.fields().size());
        assertNotNull(schema.field("field-one"));
        assertEquals(Type.STRING, schema.field("field-one").schema().type());
        assertNotNull(schema.field("field-two"));
        assertEquals(Type.STRING, schema.field("field-two").schema().type());

        assertEquals("one", struct.getString("field-one"));
        assertEquals("two", struct.getString("field-two"));
    }

    @Test
    public void shouldConvertGivenOneFieldWithArrayOfPrimitiveType() throws Exception {

        TypedValue value = converter.readJson("{\"field-one\" : [\"foo\", \"bar\"]}");

        Assert.assertNotNull(value);
        Assert.assertEquals(Type.STRUCT, value.type());
        TypedStruct struct = value.getStruct();

        StructSchema schema = struct.schema();
        assertEquals(1, schema.fields().size());
        assertNotNull(schema.field("field-one"));
        assertEquals(Type.ARRAY, schema.field("field-one").schema().type());

        assertEquals(Arrays.asList("foo", "bar"), struct.getArray("field-one"));
    }

    @Test
    public void shouldConvertGivenFieldsWithNumberType() throws Exception {

        TypedValue value = converter.readJson("{\"field-int\" : " + Integer.MAX_VALUE + ", " +
                      "\"field-long\":" + Long.MAX_VALUE + ", " +
                      "\"field-double\":" + Double.MAX_VALUE + "," +
                      "\"field-float\":" + Float.MAX_VALUE + "}");

        Assert.assertNotNull(value);
        Assert.assertEquals(Type.STRUCT, value.type());
        TypedStruct struct = value.getStruct();

        StructSchema schema = struct.schema();
        assertEquals(4, schema.fields().size());
        assertNotNull(schema.field("field-int"));
        assertEquals(Type.LONG, schema.field("field-int").type());

        assertNotNull(schema.field("field-long"));
        assertEquals(Type.LONG, schema.field("field-long").type());

        assertNotNull(schema.field("field-float"));
        assertEquals(Type.DOUBLE, schema.field("field-float").type());

        assertNotNull(schema.field("field-double"));
        assertEquals(Type.DOUBLE, schema.field("field-double").type());

        assertEquals(Integer.MAX_VALUE, struct.getLong("field-int").intValue());
        assertEquals(Long.MAX_VALUE, struct.getLong("field-long").longValue());
        assertEquals(Double.MAX_VALUE, struct.getDouble("field-double"), 0.0);
        assertEquals(Float.MAX_VALUE, struct.getDouble("field-float").floatValue(), 0.0);
    }

    @Test
    public void shouldConvertGivenFieldWithNullValue() throws Exception {
        TypedValue value = converter.readJson("{\"field-one\" : \"one\", \"field-two\":null, \"field-three\":null, \"field-four\":10, \"field-five\":\"five\"}");
        Assert.assertNotNull(value);
        Assert.assertEquals(Type.STRUCT, value.type());
        TypedStruct struct = value.getStruct();

        StructSchema schema = struct.schema();
        assertEquals(5, schema.fields().size());
        assertNotNull(schema.field("field-one"));
        assertEquals(Type.STRING, schema.field("field-one").schema().type());
        assertNotNull(schema.field("field-two"));
        assertEquals(Type.NULL, schema.field("field-two").schema().type());
        assertNotNull(schema.field("field-three"));
        assertEquals(Type.NULL, schema.field("field-three").schema().type());
        assertNotNull(schema.field("field-four"));
        assertEquals(Type.LONG, schema.field("field-four").schema().type());
        assertNotNull(schema.field("field-five"));
        assertEquals(Type.STRING, schema.field("field-five").schema().type());
    }
}