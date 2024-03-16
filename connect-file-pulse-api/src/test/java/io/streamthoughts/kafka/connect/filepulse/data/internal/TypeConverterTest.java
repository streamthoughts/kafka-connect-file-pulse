/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.data.internal;


import java.math.BigDecimal;
import org.junit.Assert;
import org.junit.Test;

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