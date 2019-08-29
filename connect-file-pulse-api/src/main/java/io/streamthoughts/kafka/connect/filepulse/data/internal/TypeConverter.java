/*
 * Copyright 2019 StreamThoughts.
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

import io.streamthoughts.kafka.connect.filepulse.data.DataException;

import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.Objects;

/**
 * Class which can be used to convert an object to a specific type.
 */
public class TypeConverter implements Serializable {

    public static Collection getArray(final Object value) throws IllegalArgumentException {
        Objects.requireNonNull(value, "value can't be null");

        if (Collection.class.isAssignableFrom(value.getClass())) {
            return (Collection) value;
        } else if (value instanceof Object[]) {
            return Arrays.asList((Object[]) value);
        }
        throw new DataException(String.format("'%s' is not assignable to Collection \"%s\"", value.getClass(), value));
    }

    public static Boolean getBool(final Object value) throws IllegalArgumentException {
        Boolean result = null;

        if (value == null) {
            result = false;
        }

        if (value instanceof String) {
            String s = (String) value;
            if (s.length() == 1 && Character.isDigit(s.charAt(0))) {
                int digit = (int) s.charAt(0);
                result = digit > 0;
            } else {
                result = s.equalsIgnoreCase("true") ||
                         s.equalsIgnoreCase("yes") ||
                         s.equalsIgnoreCase("y");
            }
        }
        if (value instanceof Boolean) {
            result = (Boolean) value;
        }

        if (result == null) {
            throw new DataException(String.format("Cannot parse boolean content from \"%s\"", value));
        }
        return result;
    }

    public static Short getShort(final Object value) throws IllegalArgumentException {
        Objects.requireNonNull(value, "value can't be null");

        if (value instanceof String && isNumber((String) value)) {
            return new BigDecimal(value.toString()).shortValue();
        }
        if (value instanceof Number) {
            Number number = (Number) value;
            return number.shortValue();
        }
        throw new DataException(String.format("Cannot parse 32-bits int content from \"%s\"", value));
    }

    public static Integer getInt(final Object value) throws IllegalArgumentException {
        Objects.requireNonNull(value, "value can't be null");

        if (value instanceof String && isNumber((String) value)) {
            return new BigDecimal(value.toString()).intValue();
        }
        if (value instanceof Number) {
            Number number = (Number) value;
            return number.intValue();
        }
        throw new DataException(String.format("Cannot parse 32-bits int content from \"%s\"", value));
    }

    public static Long getLong(final Object value) throws IllegalArgumentException {
        Objects.requireNonNull(value, "value can't be null");

        if (value instanceof String && isNumber((String) value)) {
            return new BigDecimal(value.toString()).longValue();
        }
        if (value instanceof Number) {
            Number number = (Number) value;
            return number.longValue();
        }
        throw new DataException(String.format("Cannot parse 64-bits long content from \"%s\"", value));
    }

    public static Float getFloat(final Object value) throws IllegalArgumentException {
        Objects.requireNonNull(value, "value can't be null");

        if (value instanceof String && isNumber((String) value)) {
            return new BigDecimal(value.toString()).floatValue();
        }
        if (value instanceof Number) {
            Number number = (Number) value;
            return number.floatValue();
        }
        throw new DataException(String.format("Cannot parse 32-bits float content from \"%s\"", value));
    }

    public static Double getDouble(final Object value) throws IllegalArgumentException {
        Objects.requireNonNull(value, "value can't be null");

        if (value instanceof String && isNumber((String) value)) {
            return new BigDecimal(value.toString()).doubleValue();
        }
        if (value instanceof Number) {
            Number number = (Number) value;
            return number.doubleValue();
        }
        throw new DataException(String.format("Cannot parse 64-bits double content from \"%s\"", value));
    }

    public static Date getDate(final Object value) throws IllegalArgumentException {
        Objects.requireNonNull(value, "value can't be null");

        if (value instanceof Date) {
            return (Date) value;
        }

        if (value instanceof Number) {
            Number number = (Number) value;
            return new Date(number.longValue());
        }

        if (value instanceof String && isNumber((String) value)) {
            return new Date(Long.parseLong((String) value));
        }

        throw new DataException(String.format("Cannot parse Date content from \"%s\"", value));
    }

    public static String getString(final Object value) {
        if (value instanceof ByteBuffer) {
            return StandardCharsets.UTF_8.decode((ByteBuffer)value).toString();
        }
        return (value != null) ? value.toString() : null;
    }

    public static byte[] getBytes(final Object value) {
        if (value instanceof ByteBuffer) {
            return ((ByteBuffer)value).array();
        }

        if (value instanceof String) {
            return ((String)value).getBytes(StandardCharsets.UTF_8);
        }

        if (value.getClass().isArray()) {
            return (byte[])value;
        }
        throw new DataException(String.format("Cannot parse byte[] from \"%s\"", value));
    }

    public static BigDecimal getDecimal(final Object value) {
        Objects.requireNonNull(value, "value can't be null");

        String result = null;

        if (value instanceof Double) {
            result = String.valueOf(value);
        }
        if (value instanceof Integer) {
            result = String.valueOf(value);
        }
        if (value instanceof String) {
            result = (String) value;
        }
        if (result == null) {
            throw new DataException(
                    String.format("Cannot parse decimal content from \"%s\"", value));
        }

        if (result.trim().length() == 0) {
            return null;
        }

        try {
            return new BigDecimal(result.replace(",", "."));
        } catch (NumberFormatException e) {
            throw new DataException(
                    String.format("Cannot parse decimal content from \"%s\"", value));
        }
    }

    private static boolean isNumber(final String s) {
        if (s.isEmpty()) {
            return false;
        }
        for (int i = 0; i < s.length(); i++) {
            if (i == 0 && s.charAt(i) == '-') {
                if (s.length() == 1) {
                    return false;
                } else {
                    continue;
                }
            }
            if (Character.digit(s.charAt(i), 10) < 0) {
                return false;
            }
        }
        return true;
    }
}