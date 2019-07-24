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
package io.streamthoughts.kafka.connect.filepulse.expression;

import io.streamthoughts.kafka.connect.filepulse.expression.converter.Converters;
import io.streamthoughts.kafka.connect.filepulse.expression.converter.PropertyConverter;

import java.util.List;
import java.util.Objects;
import java.util.TreeSet;

public class SubstitutionExpression implements Expression {

    private final String original;

    private final TreeSet<ReplacementExpression> replacements;

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
        this.original = original;
        this.replacements = new TreeSet<>();
        addReplacement(startIndex, endIndex, replacement);
    }

    public void addReplacement(final int startIndex,
                               final int endIndex,
                               final Expression replacement) {
        if (startIndex < 0) {
            throw new IllegalArgumentException("startIndex must be superior to 0");
        }
        if (endIndex > originalExpression().length()) {
            throw new IllegalArgumentException("endIndex must be inferior to the original expression length");
        }
        replacements.add(new ReplacementExpression(startIndex, endIndex, replacement));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String readValue(final EvaluationContext context) {
        return readValue(context, String.class);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T readValue(final EvaluationContext context, final Class<T> expectedType) {
        Objects.requireNonNull(context, "context cannot be null");
        if (isSingleSubstitution()) {
            ReplacementExpression expression = replacements.iterator().next();
            if ((expression.endIndex() - expression.startIndex()) == original.length()) {
                return expression.readValue(context, expectedType);
            }
        }

        StringBuilder sb = new StringBuilder();

        int offset = 0;
        for (ReplacementExpression replacement : replacements) {
            String replacementString = replacement.expression().readValue(context, String.class);
            if (offset < replacement.startIndex()) {
                sb.append(original, offset, replacement.startIndex());
            }
            sb.append(replacementString);
            offset = replacement.endIndex();
        }

        // copy remaining characters
        if (offset != original.length()) {
            sb.append(original, offset, original.length());
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


    private boolean isSingleSubstitution() {
        return replacements.size() == 1;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String originalExpression() {
        return original;
    }


    private static final class ReplacementExpression implements Expression, Comparable<ReplacementExpression> {

        private final int startIndex;
        private final int endIndex;
        private final Expression expression;

        /**
         * Creates a new {@link ReplacementExpression} instance.
         *
         * @param startIndex    the start index of string to substitute.
         * @param endIndex      the end index of string to substitute.
         * @param expression    the replacement to be apply on original string expression.
         */
        ReplacementExpression(final int startIndex, final int endIndex, final Expression expression) {
            Objects.requireNonNull(expression, "expression cannot be null");
            this.startIndex = startIndex;
            this.endIndex = endIndex;
            this.expression = expression;
        }

        int startIndex() {
            return startIndex;
        }

        int endIndex() {
            return endIndex;
        }

        Expression expression() {
            return expression;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Object readValue(final EvaluationContext context) {
            return expression.readValue(context);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public <T> T readValue(final EvaluationContext context, final Class<T> expectedType) {
            return expression.readValue(context, expectedType);
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
        public String originalExpression() {
            return expression.originalExpression();
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
                    ", expression=" + expression +
                    ']';
        }
    }
}
