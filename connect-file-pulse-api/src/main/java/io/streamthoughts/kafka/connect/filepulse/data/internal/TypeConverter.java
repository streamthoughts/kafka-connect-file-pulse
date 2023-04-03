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
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * Class which can be used to convert an object to a specific type.
 */
public class TypeConverter implements Serializable {

    private static final String BOOLEAN_TRUE = "true";
    private static final String BOOLEAN_FALSE = "false";

    private static final String MIN_LONG_STR_NO_SIGN = String.valueOf(Long.MIN_VALUE).substring(1);
    private static final String MAX_LONG_STR = String.valueOf(Long.MAX_VALUE);

    public static Collection getArray(final Object value) throws IllegalArgumentException {
        Objects.requireNonNull(value, "value can't be null");

        if (Collection.class.isAssignableFrom(value.getClass())) {
            return (Collection<?>) value;
        } else if (value instanceof Object[]) {
            return Arrays.asList((Object[]) value);
        }

        throw new DataException(
                String.format("'%s' is not assignable to Collection: \"%s\"", value.getClass(), value)
        );
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
                result = s.equalsIgnoreCase(BOOLEAN_TRUE) ||
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

        if (value instanceof String && isIntegerNumber((String) value)) {
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

        if (value instanceof String && isIntegerNumber((String) value)) {
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

        if (value instanceof String) {
            final String trimmed = ((String) value).trim();
            if (isIntegerNumber(trimmed)) {
                return new BigDecimal(trimmed).longValue();
            }
        }
        if (value instanceof Number) {
            Number number = (Number) value;
            return number.longValue();
        }
        throw new DataException(String.format("Cannot parse 64-bits long content from \"%s\"", value));
    }

    public static Float getFloat(final Object value) throws IllegalArgumentException {
        Objects.requireNonNull(value, "value can't be null");

        if (value instanceof String) {
            return getBigDecimal(value)
                    .map(BigDecimal::floatValue)
                    .orElseThrow(() ->
                            new DataException(String.format("Cannot parse 64-bits double content from \"%s\"", value))
                    );
        }
        if (value instanceof Number) {
            Number number = (Number) value;
            return number.floatValue();
        }
        throw new DataException(String.format("Cannot parse 32-bits float content from \"%s\"", value));
    }

    private static Optional<BigDecimal> getBigDecimal(final Object value) {
        try {
            return Optional.of(new BigDecimal(value.toString().replace(",", ".")));
        } catch (NumberFormatException e) {
            return Optional.empty();
        }
    }

    public static Double getDouble(final Object value) throws IllegalArgumentException {
        Objects.requireNonNull(value, "value can't be null");

        if (value instanceof String) {
            return getBigDecimal(value)
                    .map(BigDecimal::doubleValue)
                    .orElseThrow(() ->
                            new DataException(String.format("Cannot parse 64-bits double content from \"%s\"", value))
                    );
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

        if (value instanceof String && isIntegerNumber((String) value)) {
            return new Date(Long.parseLong((String) value));
        }

        throw new DataException(String.format("Cannot parse Date content from \"%s\"", value));
    }

    public static String getString(final Object value) {
        if (value instanceof ByteBuffer) {
            return StandardCharsets.UTF_8.decode((ByteBuffer) value).toString();
        }
        return (value != null) ? value.toString() : null;
    }

    public static byte[] getBytes(final Object value) {
        if (value instanceof ByteBuffer) {
            return ((ByteBuffer) value).array();
        }

        if (value instanceof String) {
            return ((String) value).getBytes(StandardCharsets.UTF_8);
        }

        if (value.getClass().isArray()) {
            return (byte[]) value;
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

        return getBigDecimal(value)
                .orElseThrow(() ->
                        new DataException(String.format("Cannot parse decimal content from \"%s\"", value))
                );
    }

    public static boolean isBooleanString(final String text) {
        return BOOLEAN_TRUE.equalsIgnoreCase(text) || BOOLEAN_FALSE.equalsIgnoreCase(text);
    }

    public static boolean isIntegerNumber(final String text) {
        if (text.isEmpty())
            return false;

        // skip leading negative sign, do NOT allow leading plus
        char c = text.charAt(0);
        final int start = c == '-' ? 1 : 0;
        if (start == 1 && text.length() == 1)
            return false;

        for (int i = start; i < text.length(); i++) {
            c = text.charAt(i);
            if (Character.digit(c, 10) < 0) {
                return false;
            }
        }
        return true;
    }

    public static boolean isDoubleNumber(final String text) {
        return text != null && DOUBLE_REGEX_MATCHER.matcher(text).matches();
    }

    public static boolean isInLongRange(String s) {
        final boolean negative = s.charAt(0) == '-';
        final String cmp = negative ? MIN_LONG_STR_NO_SIGN : MAX_LONG_STR;

        if (negative) {
            s = s.substring(1);
        }

        int cmpLen = cmp.length();
        int alen = s.length();
        if (alen < cmpLen) {
            return true;
        } else if (alen > cmpLen) {
            return false;
        } else {
            for(int i = 0; i < cmpLen; ++i) {
                int diff = s.charAt(i) - cmp.charAt(i);
                if (diff != 0) {
                    return diff < 0;
                }
            }
            return true;
        }
    }


    /**
     * The regexp suggested by the {@link Double#valueOf(String)}.
     */
    private static final String Digits = "(\\p{Digit}+)";
    private static final String HexDigits = "(\\p{XDigit}+)";
    // an exponent is 'e' or 'E' followed by an optionally
    // signed decimal integer.
    private static final String Exp = "[eE][+-]?" + Digits;
    private static final String fpRegex =
            "[\\x00-\\x20]*" +  // Optional leading "whitespace"
                    "[+-]?(" + // Optional sign character
                    "NaN|" +           // "NaN" string
                    "Infinity|" +      // "Infinity" string

                    // A decimal floating-point string representing a finite positive
                    // number without a leading sign has at most five basic pieces:
                    // Digits . Digits ExponentPart FloatTypeSuffix
                    //
                    // Since this method allows integer-only strings as input
                    // in addition to strings of floating-point literals, the
                    // two sub-patterns below are simplifications of the grammar
                    // productions from section 3.10.2 of
                    // The Java Language Specification.

                    // Digits ._opt Digits_opt ExponentPart_opt FloatTypeSuffix_opt
                    "(((" + Digits + "(\\.)?(" + Digits + "?)(" + Exp + ")?)|" +

                    // . Digits ExponentPart_opt FloatTypeSuffix_opt
                    "(\\.(" + Digits + ")(" + Exp + ")?)|" +

                    // Hexadecimal strings
                    "((" +
                    // 0[xX] HexDigits ._opt BinaryExponent FloatTypeSuffix_opt
                    "(0[xX]" + HexDigits + "(\\.)?)|" +

                    // 0[xX] HexDigits_opt . HexDigits BinaryExponent FloatTypeSuffix_opt
                    "(0[xX]" + HexDigits + "?(\\.)" + HexDigits + ")" +

                    ")[pP][+-]?" + Digits + "))" +
                    "[fFdD]?))" +
                    "[\\x00-\\x20]*";// Optional trailing "whitespace"
        private static final Pattern DOUBLE_REGEX_MATCHER = Pattern.compile(fpRegex);
}