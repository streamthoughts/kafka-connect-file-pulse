package io.streamthoughts.kafka.connect.filepulse.data;

import org.junit.Test;

import static org.junit.Assert.*;

public class TypedStructTest {

    private static final String STRING_FIELD_1 = "string-field-1";
    private static final String STRING_FIELD_2 = "string-field-2";
    private static final String STRING_FIELD_3 = "string-field-3";

    private static final String STRING_VALUE_1 = "string-value-1";
    private static final String STRING_VALUE_2 = "string-value-2";
    private static final String STRING_VALUE_3 = "string-value-3";

    @Test(expected = DataException.class)
    public void shouldThrowExceptionGivenInvalidFieldName() {
        TypedStruct struct = new TypedStruct();
        struct.get(STRING_FIELD_1);
    }

    @Test
    public void shouldReturnFieldPreviouslyAdded() {
        TypedStruct struct = new TypedStruct()
                .put(STRING_FIELD_1, STRING_VALUE_1);

        TypedValue typed = struct.get(STRING_FIELD_1);
        assertNotNull(typed);
        assertEquals(Schema.string(), typed.schema());
        assertEquals(STRING_VALUE_1, typed.value());
    }

    @Test
    public void shouldIncrementIndexWhilePuttingNewFields() {
        TypedStruct struct = new TypedStruct()
                .put(STRING_FIELD_1, STRING_VALUE_1)
                .put(STRING_FIELD_2, STRING_VALUE_2);

        assertEquals(0, struct.field(STRING_FIELD_1).index());
        assertEquals(1, struct.field(STRING_FIELD_2).index());
    }

    @Test
    public void shouldRemoveAndReIndexFieldsGivenValidFieldName() {
        final TypedStruct struct = new TypedStruct()
                .put(STRING_FIELD_1, STRING_VALUE_1)
                .put(STRING_FIELD_2, STRING_FIELD_2)
                .put(STRING_FIELD_3, STRING_VALUE_3);

        struct.remove(STRING_FIELD_2);

        assertFalse(struct.has(STRING_FIELD_2));
        assertEquals(0, struct.field(STRING_FIELD_1).index());
        assertEquals(1, struct.field(STRING_FIELD_3).index());
    }

    @Test
    public void shouldRenameGivenValidFieldName() {
        final TypedStruct struct = new TypedStruct()
                .put(STRING_FIELD_1, STRING_VALUE_1);

        struct.rename(STRING_FIELD_1, STRING_FIELD_2);

        assertFalse(struct.has(STRING_FIELD_1));
        assertTrue(struct.has(STRING_FIELD_2));
    }
}