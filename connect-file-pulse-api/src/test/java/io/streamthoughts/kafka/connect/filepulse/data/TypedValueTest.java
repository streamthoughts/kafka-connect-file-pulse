package io.streamthoughts.kafka.connect.filepulse.data;

import org.junit.Assert;
import org.junit.Test;

public class TypedValueTest {

    @Test
    public void should_parse_type_given_boolean_string() {
        TypedValue parsed = TypedValue.parse("true");
        Assert.assertEquals(Type.BOOLEAN, parsed.type());
        Assert.assertTrue(parsed.getBool());
    }

    @Test
    public void should_parse_type_given_numeric_string() {
        TypedValue parsed = TypedValue.parse(((Long) Long.MAX_VALUE).toString());
        Assert.assertEquals(Type.LONG, parsed.type());
        Assert.assertEquals(Long.MAX_VALUE, parsed.getLong().longValue());
    }

    @Test
    public void should_parse_type_given_too_long_numeric_string() {
        TypedValue parsed = TypedValue.parse("12345678901234567890");
        Assert.assertEquals(Type.STRING, parsed.type());
        Assert.assertEquals("12345678901234567890",  parsed.value());
    }

}