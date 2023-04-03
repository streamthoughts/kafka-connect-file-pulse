/*
 * Copyright 2021 StreamThoughts.
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
