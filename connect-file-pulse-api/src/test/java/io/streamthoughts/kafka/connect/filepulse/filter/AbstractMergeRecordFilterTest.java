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
package io.streamthoughts.kafka.connect.filepulse.filter;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class AbstractMergeRecordFilterTest {

    private static final String FIELD_VALUE_A = "a";
    private static final String FIELD_VALUE_B = "b";

    private static final String VALUE_A = "value-a";
    private static final String VALUE_B = "value-b";

    @Test
    public void shouldMergeStructGivenTwoFieldsWithDifferentName() {
        final Struct structLeft = new Struct(buildSchemaWithStringFields(FIELD_VALUE_A))
                .put(FIELD_VALUE_A, VALUE_A);

        final Struct structRight = new Struct(buildSchemaWithStringFields(FIELD_VALUE_B))
                .put(FIELD_VALUE_B, VALUE_B);

        final Struct merged = AbstractMergeRecordFilter.merge(structLeft, structRight, Collections.emptySet());

        assertNotNull(merged);

        assertEquals(VALUE_A, merged.getString(FIELD_VALUE_A));
        assertEquals(VALUE_B, merged.getString(FIELD_VALUE_B));
    }

    @Test
    public void shouldMergeStructGivenTwoFieldsWithDifferentTypeGivenOverride() {
        final Struct structLeft = new Struct(buildSchemaWithStringFields(FIELD_VALUE_A))
                .put(FIELD_VALUE_A, VALUE_A);

        final Struct structRight = new Struct(buildSchemaWithStringFields(FIELD_VALUE_A))
                .put(FIELD_VALUE_A, VALUE_B);

        final Struct merged = AbstractMergeRecordFilter.merge(structLeft, structRight, Collections.singleton(FIELD_VALUE_A));

        assertNotNull(merged);
        assertEquals(VALUE_B,  merged.getString(FIELD_VALUE_A));
    }

    @Test
    public void shouldMergeStructGivenTwoFieldsWithSameNameIntoArray() {
        final Struct structLeft = new Struct(buildSchemaWithStringFields(FIELD_VALUE_A))
                .put(FIELD_VALUE_A, VALUE_A);

        final Struct structRight = new Struct(buildSchemaWithStringFields(FIELD_VALUE_A))
                .put(FIELD_VALUE_A, VALUE_B);

        final Struct merged = AbstractMergeRecordFilter.merge(structLeft, structRight, Collections.emptySet());

        assertNotNull(merged);

        assertEquals(2, merged.getArray(FIELD_VALUE_A).size());
        assertEquals(VALUE_A,  merged.getArray(FIELD_VALUE_A).get(0));
        assertEquals(VALUE_B,  merged.getArray(FIELD_VALUE_A).get(1));
    }

    @Test
    public void shouldMergeStructGivenLeftFieldWithArrayTypeEqualToRightField() {
        final Struct structLeft = new Struct(buildSchemaWithArrayFields(FIELD_VALUE_A))
                .put(FIELD_VALUE_A, Collections.singletonList(VALUE_A));

        final Struct structRight = new Struct(buildSchemaWithStringFields(FIELD_VALUE_A))
                .put(FIELD_VALUE_A, VALUE_B);

        final Struct merged = AbstractMergeRecordFilter.merge(structLeft, structRight, Collections.emptySet());

        assertNotNull(merged);

        assertEquals(2, merged.getArray(FIELD_VALUE_A).size());
        assertEquals(VALUE_A,  merged.getArray(FIELD_VALUE_A).get(0));
        assertEquals(VALUE_B,  merged.getArray(FIELD_VALUE_A).get(1));
    }

    @Test
    public void shouldMergeStructGivenRightFieldWithArrayTypeEqualToLeftField() {
        final Struct structLeft = new Struct(buildSchemaWithStringFields(FIELD_VALUE_A))
                .put(FIELD_VALUE_A, VALUE_A);

        final Struct structRight = new Struct(buildSchemaWithArrayFields(FIELD_VALUE_A))
                .put(FIELD_VALUE_A, Collections.singletonList(VALUE_B));

        final Struct merged = AbstractMergeRecordFilter.merge(structLeft, structRight, Collections.emptySet());

        assertNotNull(merged);

        assertEquals(2, merged.getArray(FIELD_VALUE_A).size());
        assertEquals(VALUE_A,  merged.getArray(FIELD_VALUE_A).get(0));
        assertEquals(VALUE_B,  merged.getArray(FIELD_VALUE_A).get(1));
    }

    @Test
    public void shouldMergeStructGivenTwoArrayFieldsWithEqualsValueType() {
        final Struct structLeft =  new Struct(buildSchemaWithArrayFields(FIELD_VALUE_A))
                .put(FIELD_VALUE_A, Collections.singletonList(VALUE_A));

        final Struct structRight = new Struct(buildSchemaWithArrayFields(FIELD_VALUE_A))
                .put(FIELD_VALUE_A, Collections.singletonList(VALUE_B));

        final Struct merged = AbstractMergeRecordFilter.merge(structLeft, structRight, Collections.emptySet());

        assertNotNull(merged);

        assertEquals(2, merged.getArray(FIELD_VALUE_A).size());
        assertEquals(VALUE_A,  merged.getArray(FIELD_VALUE_A).get(0));
        assertEquals(VALUE_B,  merged.getArray(FIELD_VALUE_A).get(1));
    }

    private Schema buildSchemaWithArrayFields(final String ... fields) {
        SchemaBuilder sb = SchemaBuilder.struct();
        Arrays.stream(fields)
                .forEach(s -> sb.field(s, SchemaBuilder.array(SchemaBuilder.string())));
        return sb.build();
    }

    private Schema buildSchemaWithStringFields(final String ... fields) {
        SchemaBuilder sb = SchemaBuilder.struct();
        Arrays.stream(fields)
              .forEach(s -> sb.field(s, Schema.STRING_SCHEMA));
        return sb.build();
    }

}