/*
 * Copyright 2023 StreamThoughts.
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