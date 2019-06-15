/*
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

import io.streamthoughts.kafka.connect.filepulse.source.FileInputData;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.*;

public class DefaultJSONStructConverterTest {

    private JSONStructConverter converter = new DefaultJSONStructConverter();

    @Test
    public void shouldConvertGivenFieldsWithStringType() {

        FileInputData output = converter.readJson("{\"field-one\" : \"one\", \"field-two\":\"two\"}");

        Assert.assertNotNull(output);
        Struct struct = output.value();

        Schema schema = struct.schema();
        assertEquals(2, schema.fields().size());
        assertNotNull(schema.field("field-one"));
        assertEquals(Schema.Type.STRING, schema.field("field-one").schema().type());
        assertNotNull(schema.field("field-two"));
        assertEquals(Schema.Type.STRING, schema.field("field-two").schema().type());

        assertEquals("one", struct.getString("field-one"));
        assertEquals("two", struct.getString("field-two"));
    }

    @Test
    public void shouldConvertGivenOneFieldWithArrayOfPrimitiveType() {

        FileInputData output = converter.readJson("{\"field-one\" : [\"foo\", \"bar\"]}");

        Assert.assertNotNull(output);
        Struct struct = output.value();

        Schema schema = struct.schema();
        assertEquals(1, schema.fields().size());
        assertNotNull(schema.field("field-one"));
        assertEquals(Schema.Type.ARRAY, schema.field("field-one").schema().type());

        assertEquals(Arrays.asList("foo", "bar"), struct.getArray("field-one"));
    }

    @Test
    public void shouldConvertGivenFieldsWithNumberType() {

        FileInputData output = converter.readJson("{\"field-int\" : " + Integer.MAX_VALUE + ", " +
                      "\"field-long\":" + Long.MAX_VALUE + ", " +
                      "\"field-double\":" + Double.MAX_VALUE + "," +
                      "\"field-float\":" + Float.MAX_VALUE + "}");

        Assert.assertNotNull(output);
        Struct struct = output.value();

        Schema schema = struct.schema();
        assertEquals(4, schema.fields().size());
        assertNotNull(schema.field("field-int"));
        assertEquals(Schema.Type.INT64, schema.field("field-int").schema().type());

        assertNotNull(schema.field("field-long"));
        assertEquals(Schema.Type.INT64, schema.field("field-long").schema().type());

        assertNotNull(schema.field("field-float"));
        assertEquals(Schema.Type.FLOAT64, schema.field("field-float").schema().type());

        assertNotNull(schema.field("field-double"));
        assertEquals(Schema.Type.FLOAT64, schema.field("field-double").schema().type());

        assertEquals(Integer.MAX_VALUE, struct.getInt64("field-int").intValue());
        assertEquals(Long.MAX_VALUE, struct.getInt64("field-long").longValue());
        assertEquals(Double.MAX_VALUE, struct.getFloat64("field-double"), 0.0);
        assertEquals(Float.MAX_VALUE, struct.getFloat64("field-float").floatValue(), 0.0);

    }

}