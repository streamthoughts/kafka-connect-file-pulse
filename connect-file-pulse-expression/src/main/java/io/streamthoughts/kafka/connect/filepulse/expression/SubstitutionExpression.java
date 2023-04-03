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
package io.streamthoughts.kafka.connect.filepulse.expression;

import io.streamthoughts.kafka.connect.filepulse.data.TypedValue;
import io.streamthoughts.kafka.connect.filepulse.expression.converter.Converters;
import io.streamthoughts.kafka.connect.filepulse.expression.converter.PropertyConverter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class SubstitutionExpression extends AbstractExpression {

    private final TreeSet<ReplacementExpression> replacements;

    /**
     * Creates a new {@link SubstitutionExpression}.
     *
     * @param original      the original expression.
     */
    public SubstitutionExpression(final String original) {
        super(original);
        this.replacements = new TreeSet<>();
    }

    /**
     * Creates a new {@link SubstitutionExpression}.
     *
     * @param original      the original expression.
     * @param startIndex    the start index of string to substitute.
     * @param endIndex      the end index of string to substitute.
     * @param replacement   the replacement to be apply on original string expression.
     */
    public SubstitutionExpression(final String original,
                                  final int startIndex,
                                  final int endIndex,
                                  final Expression replacement) {
        super(original);
        this.replacements = new TreeSet<>();
        addReplacement(startIndex, endIndex, replacement);
    }

    public void addReplacement(final int startIndex,
                               final int endIndex,
                               final Expression replacement) {
        addReplacement(
            new ReplacementExpression(
                "",
                startIndex,
                endIndex,
                Collections.singletonList(replacement))
        );
    }

    public void addReplacement(final ReplacementExpression expression) {
        if (expression.startIndex < 0) {
            throw new IllegalArgumentException("startIndex must be superior to 0");
        }
        if (expression.endIndex > originalExpression().length()) {
            throw new IllegalArgumentException(
                    "endIndex must be inferior to the original expression length " +
                            "(" + originalExpression().length() +"): " + expression.endIndex);
        }
        replacements.add(expression);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object readValue(final EvaluationContext context) {
        return Optional.ofNullable(readValue(context, TypedValue.class)).map(TypedValue::value).orElse(null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T readValue(final EvaluationContext context, final Class<T> expectedType) {
        Objects.requireNonNull(context, "context cannot be null");
        if (isSingleSubstitution()) {
            ReplacementExpression expression = replacements.iterator().next();
            if ((expression.endIndex() - expression.startIndex()) == originalExpression().length()) {
                return expression.readValue(context, expectedType);
            }
        }

        StringBuilder sb = new StringBuilder();

        int offset = 0;
        for (ReplacementExpression replacement : replacements) {
            String replacementString = replacement.readValue(context, String.class);
            if (offset < replacement.startIndex()) {
                sb.append(originalExpression(), offset, replacement.startIndex());
            }
            sb.append(replacementString);
            offset = replacement.endIndex();
        }

        // copy remaining characters
        if (offset != originalExpression().length()) {
            sb.append(originalExpression(), offset, originalExpression().length());
        }

        final String value = sb.toString();

        final List<PropertyConverter> converters = context.getPropertyConverter();
        return Converters.converts(converters, value, expectedType);
    }

    /**
     * {@inheritDoc}
     */
    public void writeValue(final Object value, final EvaluationContext context) {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean canWrite() {
        return false;
    }

    private boolean isSingleSubstitution() {
        return replacements.size() == 1;
    }

    public TreeSet<ReplacementExpression> getReplacements() {
        return replacements;
    }

    public static final class ReplacementExpression
            extends AbstractExpression
            implements Comparable<ReplacementExpression> {

        private final int startIndex;
        private final int endIndex;
        private final List<Expression> expressions;

        /**
         * Creates a new {@link ReplacementExpression} instance.
         *
         * @param originalExpression the original string expression.
         * @param startIndex         the start index of string to substitute.
         * @param endIndex           the end index of string to substitute.
         * @param expressions        the replacements to be apply on original string expression.
         */
        public ReplacementExpression(final String originalExpression,
                                     final int startIndex,
                                     final int endIndex,
                                     final List<Expression> expressions) {
            super(originalExpression);
            this.expressions =  Objects.requireNonNull(expressions, "expressions cannot be null");
            this.startIndex = startIndex;
            this.endIndex = endIndex;
        }

        public int startIndex() {
            return startIndex;
        }

        public int endIndex() {
            return endIndex;
        }

        public List<Expression> expressions() {
            return expressions;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Object readValue(final EvaluationContext context) {
            List<TypedValue> values = new ArrayList<>(expressions.size());
            for (Expression replacement : expressions) {
                values.add(replacement.readValue(context, TypedValue.class));
            }
            if (values.size() == 1) {
                final TypedValue typed = values.get(0);
                return typed == null ? null : typed.value();
            }
            return values.stream().map(TypedValue::getString).collect(Collectors.joining());
        }

        /**
         * {@inheritDoc}
         */
        @Override
        @SuppressWarnings("unchecked")
        public <T> T readValue(final EvaluationContext context, final Class<T> expectedType) {
            final Object returned = readValue(context);
            if (returned == null)
                return null;

            if (expectedType.isAssignableFrom(returned.getClass()))
                return (T)returned;

            final List<PropertyConverter> converters = context.getPropertyConverter();
            return Converters.converts(converters, returned, expectedType);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void writeValue(final Object value, final EvaluationContext context) {
            throw new UnsupportedOperationException();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean canWrite() {
            return false;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public int compareTo(final ReplacementExpression that) {
            Objects.requireNonNull(that, "cannot compare to null object");
            return Integer.compare(startIndex, that.startIndex);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public String toString() {
            return "[" +
                    "startIndex=" + startIndex +
                    ", endIndex=" + endIndex +
                    ", expressions=" + expressions +
                    ']';
        }
    }

    @Override
    public String toString() {
        return "[" +
                "originalExpression=" + originalExpression() +
                ", replacements=" + replacements +
                ']';
    }
}
