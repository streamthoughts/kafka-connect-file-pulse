/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.expression.function.datetime;

import io.streamthoughts.kafka.connect.filepulse.data.TypedValue;
import io.streamthoughts.kafka.connect.filepulse.expression.Expression;
import io.streamthoughts.kafka.connect.filepulse.expression.ExpressionException;
import io.streamthoughts.kafka.connect.filepulse.expression.ValueExpression;
import io.streamthoughts.kafka.connect.filepulse.expression.function.AbstractExpressionFunctionInstance;
import io.streamthoughts.kafka.connect.filepulse.expression.function.Arguments;
import io.streamthoughts.kafka.connect.filepulse.expression.function.EvaluatedExecutionContext;
import io.streamthoughts.kafka.connect.filepulse.expression.function.ExpressionFunction;
import io.streamthoughts.kafka.connect.filepulse.internal.DateTimeParser;
import java.time.ZoneId;
import java.time.ZonedDateTime;

public class ToTimestamp implements ExpressionFunction {

    /**
     * {@inheritDoc}
     */
    @Override
    public Instance get() {
        return new AbstractExpressionFunctionInstance() {

            private DateTimeParser parser;

            private ZoneId zoneId = ZoneId.systemDefault();

            private String syntax() {
                return String.format("syntax %s(<datetime_expr>, <pattern> [, timezone]", name());
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

                if (args.length == 3) {
                    zoneId = ZoneId.of(((ValueExpression)args[2]).value().getString());
                }

                parser = new DateTimeParser(((ValueExpression)args[1]).value().getString());

                return Arguments.of("datetime", args[0]);
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public TypedValue invoke(final EvaluatedExecutionContext context) throws ExpressionException {
                final String datetime = context.get(0).getString();
                final ZonedDateTime zdt = parser.parse(datetime, zoneId);
                return TypedValue.int64(zdt.toInstant().toEpochMilli());
            }
        };
    }
}
