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
package io.streamthoughts.kafka.connect.filepulse.data.merger;

import io.streamthoughts.kafka.connect.filepulse.data.ArraySchema;
import io.streamthoughts.kafka.connect.filepulse.data.DataException;
import io.streamthoughts.kafka.connect.filepulse.data.FieldPaths;
import io.streamthoughts.kafka.connect.filepulse.data.Schema;
import io.streamthoughts.kafka.connect.filepulse.data.Type;
import io.streamthoughts.kafka.connect.filepulse.data.TypedField;
import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.data.TypedValue;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * Default {@link TypeValueMerger} implementation which automatically
 * merge fields with the same name into a new array field.
 */
public class DefaultTypeValueMerger implements TypeValueMerger {

    /**
     * Straightforward method to merge two {@link TypedStruct} instances.
     *
     * @param left      the left {@link TypedStruct} to be merged.
     * @param right     the right {@link TypedStruct} to be merged.
     * @param overwrite the left field that must overwritten.
     * @return the new {@link TypedStruct} instance.
     */
    public TypedStruct merge(final TypedStruct left,
                             final TypedStruct right,
                             final Set<String> overwrite) {
        return mergeObjects(left, right, FieldPaths.from(overwrite));
    }

    private static TypedStruct mergeObjects(final TypedStruct left,
                                            final TypedStruct right,
                                            final FieldPaths overwrite) {
        if (left == null) return right;
        if (right == null) return left;

        final TypedStruct struct = TypedStruct.create();

        for (TypedField leftField : left) {
            final String fieldName = leftField.name();

            final TypedValue leftValue = left.get(leftField);

            if (!right.has(fieldName)) {
                struct.put(fieldName, leftValue);
                continue;
            }

            boolean isOverwrite = overwrite.anyMatches(fieldName);

            if (isOverwrite) {
                continue; // skip the left field
            }

            final TypedField rightField = right.field(fieldName);
            checkIfTypesAreCompatibleForMerge(leftField, rightField);

            final TypedValue rightValue = right.get(rightField);
            final TypedValue merged;

            if (leftField.type() == Type.STRUCT) {
                final TypedStruct mergedStruct = mergeObjects(
                    leftValue.getStruct(),
                    rightValue.getStruct(),
                        overwrite.next(fieldName)
                );
                merged = TypedValue.struct(mergedStruct);
            } else {
                merged = merge(leftValue, rightValue);
            }
            struct.put(fieldName, merged);
        }

        for (TypedField f : right) {
            if (!struct.has(f.name())) {
                struct.put(f, right.get(f));
            }
        }
        return struct;
    }

    private static TypedValue merge(final TypedValue left, final TypedValue right) {
        List<Object> values = new LinkedList<>();

        if (left.type() == Type.ARRAY) {
            values.addAll(left.getArray());
        } else {
            values.add(left.value());
        }

        if (right.type() == Type.ARRAY) {
            values.addAll(right.getArray());
        } else {
            values.add(right.value());
        }

        return TypedValue.array(values, left.schema());
    }

    private static void checkIfTypesAreCompatibleForMerge(final TypedField left, final TypedField right) {
        final String name = left.name();

        final Schema leftSchema = left.schema();
        final Schema rightSchema = right.schema();

        if (left.type() == Type.ARRAY && right.type() == Type.ARRAY) {
            final Schema valueSchemaLeft = ((ArraySchema) leftSchema).valueSchema();
            final Schema valueSchemaRight = ((ArraySchema) rightSchema).valueSchema();
            if (isTypeNotEqual(valueSchemaLeft, valueSchemaRight)) {
                throw new DataException(
                    String.format(
                        "Cannot merge fields '%s' of type array with different value types : Array[%s]<>Array[%s]",
                        name,
                        valueSchemaLeft.type(),
                        valueSchemaRight.type()
                    )
                );
            }
            return;
        }

        if (left.type() == Type.ARRAY) {
            Schema valueSchemaLeft = ((ArraySchema) leftSchema).valueSchema();
            if (isTypeNotEqual(valueSchemaLeft, rightSchema)) {
                throw new DataException(
                    String.format(
                        "Cannot merge fields '%s' with different array value types : Array[%s]<>%s",
                        name,
                        valueSchemaLeft.type(),
                        rightSchema.type()
                    )
                );
            }
            return;
        }

        if (right.type() == Type.ARRAY) {
            Schema valueSchemaRight = ((ArraySchema) rightSchema).valueSchema();
            if (isTypeNotEqual(leftSchema, valueSchemaRight)) {
                throw new DataException(
                    String.format(
                        "Cannot merge fields '%s' with different array value types : %s<>Array[%s]",
                        name,
                        leftSchema.type(),
                        valueSchemaRight.type()
                    )
                );
            }
            return;
        }

        // neither left or right is of type array
        if(isTypeNotEqual(leftSchema, rightSchema)) {
            throw new DataException(
                String.format(
                    "Cannot merge fields '%s' with different types : %s<>%s",
                    name,
                    leftSchema.type(),
                    rightSchema.type()
                )
            );
        }
    }

    private static boolean isTypeNotEqual(final Schema left, final Schema right) {
        return left != null && right != null && left.type() != right.type();
    }

}
