package io.streamthoughts.kafka.connect.filepulse.expression.function.impl;

import io.streamthoughts.kafka.connect.filepulse.data.TypeValue;
import io.streamthoughts.kafka.connect.filepulse.expression.function.SimpleArguments;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

public class ExistsTest {

    private static final Schema SCHEMA = SchemaBuilder
        .struct()
        .field("field", Schema.STRING_SCHEMA)
        .build();

    private static final SchemaAndValue NON_EMPTY_STRUCT = new SchemaAndValue(
            SCHEMA,
            new Struct(SCHEMA));

    private static final SchemaAndValue EMPTY_STRUCT = new SchemaAndValue(
            SchemaBuilder.struct(),
            new Struct(SchemaBuilder.struct()));

    private final Exists exists = new Exists();

    @Test
    public void shouldAcceptGivenStringSchemaAndValue() {
        Assert.assertTrue(exists.accept(EMPTY_STRUCT));
    }

    @Test
    public void shouldReturnFalseGivenEmptyStruct() {
        SimpleArguments arguments = exists.prepare(new TypeValue[]{TypeValue.of("field")});

        SchemaAndValue output = exists.apply(EMPTY_STRUCT, arguments);
        assertEquals(Schema.BOOLEAN_SCHEMA, output.schema());
        assertFalse((boolean)output.value());
    }

    @Test
    public void shouldReturnTrueGivenStructWithExpectedField() {
        SimpleArguments arguments = exists.prepare(new TypeValue[]{TypeValue.of("field")});
        SchemaAndValue output = exists.apply(NON_EMPTY_STRUCT, arguments);
        assertEquals(Schema.BOOLEAN_SCHEMA, output.schema());
        assertTrue((boolean)output.value());
    }
}