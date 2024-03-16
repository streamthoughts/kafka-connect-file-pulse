/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.expression.function.strings;

import io.streamthoughts.kafka.connect.filepulse.data.Type;
import io.streamthoughts.kafka.connect.filepulse.data.TypedValue;
import io.streamthoughts.kafka.connect.filepulse.expression.Expression;
import io.streamthoughts.kafka.connect.filepulse.expression.ExpressionException;
import io.streamthoughts.kafka.connect.filepulse.expression.ValueExpression;
import io.streamthoughts.kafka.connect.filepulse.expression.function.AbstractExpressionFunctionInstance;
import io.streamthoughts.kafka.connect.filepulse.expression.function.Arguments;
import io.streamthoughts.kafka.connect.filepulse.expression.function.EvaluatedExecutionContext;
import io.streamthoughts.kafka.connect.filepulse.expression.function.ExpressionFunction;
import io.streamthoughts.kafka.connect.filepulse.internal.StringUtils;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.Function;
import java.util.regex.Pattern;

public class Split implements ExpressionFunction {

    /**
     * {@inheritDoc}
     */
    @Override
    public ExpressionFunction.Instance get() {
        return new SplitInstance(name());
    }

    static class SplitInstance extends AbstractExpressionFunctionInstance {

        private static final String FIELD_ARG = "field_expr";
        private static final String REGEX_ARG = "separator";
        private static final String LIMIT_ARG = "limit";

        private final String name;

        private Function<String, String[]> function;

        SplitInstance(final String name) {
            this.name = Objects.requireNonNull(name, "'name' should not be null");
        }

        private String syntax() {
            return String.format("syntax %s(%s, %s [, %s])",  name, FIELD_ARG, REGEX_ARG, LIMIT_ARG);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Arguments prepare(final Expression[] args) {
            if (args.length > 3) {
                throw new ExpressionException("Too many arguments: " + syntax());
            }
            if (args.length < 2) {
                throw new ExpressionException("Missing required arguments: " + syntax());
            }

            final int limit = args.length == 3 ? ((ValueExpression) args[2]).value().getInt() : 0;

            final String regex = ((ValueExpression) args[1]).value().getString();
            if (StringUtils.isFastSplit(regex)) {
                function = s -> s.split(regex, limit);
            } else {
                final Pattern splitPattern = Pattern.compile(regex);
                function = s -> splitPattern.split(s, limit);
            }

            return Arguments.of(FIELD_ARG, args[0]);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public TypedValue invoke(final EvaluatedExecutionContext context) throws ExpressionException {
            final String field = context.get(0).getString();
            return TypedValue.array(Arrays.asList(function.apply(field)), Type.STRING);
        }
    }
}
