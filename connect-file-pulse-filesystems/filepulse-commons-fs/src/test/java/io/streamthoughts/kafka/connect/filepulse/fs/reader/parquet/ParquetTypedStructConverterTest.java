/*
 * Copyright 2024 StreamThoughts.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.streamthoughts.kafka.connect.filepulse.fs.reader.parquet;

import java.util.ArrayList;
import java.util.List;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.schema.*;
import org.junit.Assert;
import org.junit.Test;

public class ParquetTypedStructConverterTest {

    private static final String STRING_VALUE = "test";
    private static final int INT_VALUE = 123;
    private static final double DOUBLE_VALUE = 1.2D;
    private static final long LONG_VALUE = 123L;
    private static final boolean BOOLEAN_VALUE = true;
    private static final float FLOAT_VALUE = 123F;
    private SimpleGroup simpleGroup;
    private SimpleGroup baseArraySimpleGroup;
    private final List<Type> listType = new ArrayList<>();

    @Test
    public void check_string_value_converter() {
        var stringValue = new PrimitiveType(Type.Repetition.REPEATED, PrimitiveType.PrimitiveTypeName.BINARY, "test");
        listType.add(stringValue);
        simpleGroup = new SimpleGroup(new GroupType(Type.Repetition.REPEATED, "name", listType));
        simpleGroup.add(0, STRING_VALUE);
        var typedStruct = ParquetTypedStructConverter.fromParquetFileReader(simpleGroup);

        Assert.assertEquals(STRING_VALUE, typedStruct.get("test").getString());
    }
    @Test
    public void check_int_value_converter() {
        var integerValue = new PrimitiveType(Type.Repetition.REPEATED, PrimitiveType.PrimitiveTypeName.INT32, "integer");
        listType.add(0, integerValue);
        simpleGroup = new SimpleGroup(new GroupType(Type.Repetition.REPEATED, "name", listType));
        simpleGroup.add(0, INT_VALUE);
        var typedStruct = ParquetTypedStructConverter.fromParquetFileReader(simpleGroup);

        Assert.assertEquals(INT_VALUE, typedStruct.get("integer").getInt().intValue());
    }

    @Test
    public void check_double_value_converter() {
        var doubleValue = new PrimitiveType(Type.Repetition.REPEATED, PrimitiveType.PrimitiveTypeName.DOUBLE, "double");
        listType.add(0, doubleValue);
        simpleGroup = new SimpleGroup(new GroupType(Type.Repetition.REPEATED, "name", listType));
        simpleGroup.add(0, DOUBLE_VALUE);
        var typedStruct = ParquetTypedStructConverter.fromParquetFileReader(simpleGroup);

        Assert.assertEquals(DOUBLE_VALUE, typedStruct.get("double").getDouble(), 0);
    }

    @Test
    public void check_long_value_converter() {
        var longValue = new PrimitiveType(Type.Repetition.REPEATED, PrimitiveType.PrimitiveTypeName.INT64, "long");
        listType.add(0, longValue);
        simpleGroup = new SimpleGroup(new GroupType(Type.Repetition.REPEATED, "name", listType));
        simpleGroup.add(0, LONG_VALUE);
        var typedStruct = ParquetTypedStructConverter.fromParquetFileReader(simpleGroup);

        Assert.assertEquals(LONG_VALUE, typedStruct.get("long").getLong().longValue());
    }

    @Test
    public void check_boolean_value_converter() {
        var booleanValue = new PrimitiveType(Type.Repetition.REPEATED, PrimitiveType.PrimitiveTypeName.BOOLEAN, "boolean");
        listType.add(0, booleanValue);
        simpleGroup = new SimpleGroup(new GroupType(Type.Repetition.REPEATED, "name", listType));
        simpleGroup.add(0, BOOLEAN_VALUE);
        var typedStruct = ParquetTypedStructConverter.fromParquetFileReader(simpleGroup);

        Assert.assertEquals(BOOLEAN_VALUE, typedStruct.get("boolean").getBool());
    }

    @Test
    public void check_float_value_converter() {
        var floatValue = new PrimitiveType(Type.Repetition.REPEATED, PrimitiveType.PrimitiveTypeName.FLOAT, "float");
        listType.add(0, floatValue);
        simpleGroup = new SimpleGroup(new GroupType(Type.Repetition.REPEATED, "name", listType));
        simpleGroup.add(0, FLOAT_VALUE);
        var typedStruct = ParquetTypedStructConverter.fromParquetFileReader(simpleGroup);

        Assert.assertEquals(FLOAT_VALUE, typedStruct.get("float").getFloat(), 0);
    }

    @Test
    public void check_array_value_converter() {
        listType.add(0, generateArray());
        simpleGroup = new SimpleGroup(new GroupType(Type.Repetition.REPEATED, "name", listType));
        simpleGroup.add(0, baseArraySimpleGroup);

        var typedStruct = ParquetTypedStructConverter.fromParquetFileReader(simpleGroup);

        Assert.assertEquals(2, typedStruct.get("LIST").getArray().size());
    }

    @Test
    public void check_array_value_converter_when_array_is_empty() {
        listType.add(0, generateEmptyArray());
        simpleGroup = new SimpleGroup(new GroupType(Type.Repetition.REPEATED, "name", listType));
        simpleGroup.add(0, baseArraySimpleGroup);

        var typedStruct = ParquetTypedStructConverter.fromParquetFileReader(simpleGroup);

        Assert.assertEquals(0, typedStruct.get("EMPTY_LIST").getArray().size());
    }

    @Test
    public void check_value_converter_when_repetition_count_equals_0() {
        listType.add(0, generateFieldRepetitionCountEmpty());
        simpleGroup = new SimpleGroup(new GroupType(Type.Repetition.REPEATED, "name", listType));

        var typedStruct = ParquetTypedStructConverter.fromParquetFileReader(simpleGroup);

        Assert.assertTrue(typedStruct.get("REPLICATION_EMPTY").isNull());
    }

    private GroupType generateArray() {
        var elementList = new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.INT32, "element");

        var dataList = new GroupType(Type.Repetition.OPTIONAL, "LIST", elementList);

        var dataGroup1 = new SimpleGroup(dataList);
        var dataGroup2 = new SimpleGroup(dataList);

        dataGroup1.add(0, 112345);
        dataGroup2.add(0, 4567);

        GroupType schemaGroup = new GroupType(Type.Repetition.OPTIONAL, "element", List.of(dataList));
        baseArraySimpleGroup = new SimpleGroup(schemaGroup);
        baseArraySimpleGroup.add(0, dataGroup1);
        baseArraySimpleGroup.add(0, dataGroup2);
        return ConversionPatterns.listOfElements(Type.Repetition.REPEATED, "LIST", schemaGroup);
    }

    private GroupType generateEmptyArray() {
        var elementList = new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.INT32, "element");
        var dataList = new GroupType(Type.Repetition.OPTIONAL, "LIST", elementList);

        var schemaGroup = new GroupType(Type.Repetition.OPTIONAL, "element", List.of(dataList));
        baseArraySimpleGroup = new SimpleGroup(schemaGroup);
        return ConversionPatterns.listOfElements(Type.Repetition.REPEATED, "EMPTY_LIST", schemaGroup);
    }

    private GroupType generateFieldRepetitionCountEmpty() {
        var schemaGroup = new GroupType(Type.Repetition.OPTIONAL, "element");
        return ConversionPatterns.listOfElements(Type.Repetition.REPEATED, "REPLICATION_EMPTY", schemaGroup);
    }
}