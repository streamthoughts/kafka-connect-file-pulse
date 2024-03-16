/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.fs.reader.parquet;

import static io.streamthoughts.kafka.connect.filepulse.data.TypedValue.bool;
import static io.streamthoughts.kafka.connect.filepulse.data.TypedValue.float32;
import static io.streamthoughts.kafka.connect.filepulse.data.TypedValue.float64;
import static io.streamthoughts.kafka.connect.filepulse.data.TypedValue.int32;
import static io.streamthoughts.kafka.connect.filepulse.data.TypedValue.int64;
import static io.streamthoughts.kafka.connect.filepulse.data.TypedValue.string;

import io.streamthoughts.kafka.connect.filepulse.data.Type;
import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.data.TypedValue;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.function.TriFunction;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.PrimitiveType;


/**
 * The {@link ParquetTypedStructConverter} can be used to convert a parquet object into a {@link TypedStruct}.
 */
public class ParquetTypedStructConverter {

    private static final Map<String, TriFunction<String, SimpleGroup, Integer, TypedValue>> PARQUET_TYPES_TO_CONVERTER;

    static {
        PARQUET_TYPES_TO_CONVERTER = new HashMap<>();
        PARQUET_TYPES_TO_CONVERTER.put("BOOLEAN",
                (type, value, integer) -> bool(value.getBoolean(integer, 0)));
        PARQUET_TYPES_TO_CONVERTER.put("INT32",
                (type, value, integer) -> int32(value.getInteger(integer, 0)));
        PARQUET_TYPES_TO_CONVERTER.put("INT64",
                (type, value, integer) -> int64(value.getLong(integer, 0)));
        PARQUET_TYPES_TO_CONVERTER.put("FLOAT",
                (type, value, integer) -> float32(value.getFloat(integer, 0)));
        PARQUET_TYPES_TO_CONVERTER.put("DOUBLE",
                (type, value, integer) -> float64(value.getDouble(integer, 0)));
        PARQUET_TYPES_TO_CONVERTER.put("BINARY",
                (type, value, integer) -> string(value.getValueToString(integer, 0)));
        PARQUET_TYPES_TO_CONVERTER.put("STRING",
                (type, value, integer) -> string(value.getValueToString(integer, 0)));
        PARQUET_TYPES_TO_CONVERTER.put("LIST", ParquetTypedStructConverter::list);
    }

    /**
     * Converts the specified {@link SimpleGroup} instance into a {@link SimpleGroup}.
     *
     * @return a new {@link TypedStruct} instance.
     */
    public static TypedStruct fromParquetFileReader(final SimpleGroup simpleGroup) {
        TypedStruct struct = TypedStruct.create();
        GroupType group = simpleGroup.getType();
        for (int i = 0; i < group.getFieldCount(); i++) {
            org.apache.parquet.schema.Type field = group.getType(i);
            String filedType = field instanceof PrimitiveType ?
                field.asPrimitiveType().getPrimitiveTypeName().name() :
                field.getLogicalTypeAnnotation().toString();
            struct.put(field.getName(), getTypedValueFromSimpleGroup(filedType, simpleGroup, i));
        }
        return struct;
    }

    /**
     * Converts the specified {@link SimpleGroup} instance into a {@link TypedValue}.
     *
     * @param fieldName   type of field
     * @param simpleGroup data of parquet file
     * @param i           sequence number of the value
     * @return a new {@link TypedValue} instance.
     */
    private static TypedValue list(String fieldName, SimpleGroup simpleGroup, int i) {
        Group group = simpleGroup.getGroup(i, 0);
        // the number of items in the list
        int elements = group.getFieldRepetitionCount(0);
        List<Object> resultList = new ArrayList<>(elements);
        Type fieldType = null;
        if (elements > 0) {
            for (int k = 0; k < elements; k++) {
                //Get a group of a list element
                Group subGroup = group.getGroup(0, k);
                //Get the name of the field type of the list element
                String fieldTypeString = subGroup
                    .getType()
                    .getType(0)
                    .asPrimitiveType().getPrimitiveTypeName().name();
                //Convert list element into TypedValue
                TypedValue value = getTypedValueFromSimpleGroup(fieldTypeString, (SimpleGroup) subGroup, 0);
                //Get the array element type, which will be the list type
                if (Optional.ofNullable(fieldType).isEmpty()) {
                    fieldType = value.type();
                }
                resultList.add(value.value());
            }
            return TypedValue.array(resultList, fieldType);
        }
        return TypedValue.array(List.of(), Type.ARRAY);
    }

    private static TypedValue getTypedValueFromSimpleGroup(String fieldTypeString, SimpleGroup simpleGroup, int i) {
        var converter = PARQUET_TYPES_TO_CONVERTER.get(fieldTypeString);
        if (converter == null) {
            throw new UnsupportedOperationException("Not supported type in Parquet convertor: " + fieldTypeString);
        }
        if (simpleGroup.getFieldRepetitionCount(i) > 0) {
            return converter.apply(fieldTypeString, simpleGroup, i);
        }
        return TypedValue.none();
    }
}
