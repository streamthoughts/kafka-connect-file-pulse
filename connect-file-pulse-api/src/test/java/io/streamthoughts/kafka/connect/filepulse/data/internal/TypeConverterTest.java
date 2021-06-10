package io.streamthoughts.kafka.connect.filepulse.data.internal;


import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;

public class TypeConverterTest {

    @Test
    public void should_convert_decimal_number() {
        final BigDecimal result = TypeConverter.getDecimal("1.10");
        Assert.assertEquals(1.10, result.doubleValue(), 0.1);
    }

    @Test
    public void should_convert_decimal_number_given_comma() {
        final BigDecimal result = TypeConverter.getDecimal("1,101");
        Assert.assertEquals(1.101, result.doubleValue(), 0);
    }

    @Test
    public void should_convert_double_number() {
        final Double result = TypeConverter.getDouble("1.101");
        Assert.assertEquals(1.101, result.doubleValue(), 0);
    }

    @Test
    public void should_convert_double_number_given_comma() {
        final Double result = TypeConverter.getDouble("1,101");
        Assert.assertEquals(1.101, result.doubleValue(), 0);
    }

    @Test
    public void should_convert_float_number() {
        final Float result = TypeConverter.getFloat("1.101");
        Assert.assertEquals(1.101f, result.floatValue(), 0);
    }


}