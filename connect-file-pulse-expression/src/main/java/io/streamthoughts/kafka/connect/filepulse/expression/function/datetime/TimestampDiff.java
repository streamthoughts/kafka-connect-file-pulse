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

import io.streamthoughts.kafka.connect.filepulse.annotation.VisibleForTesting;
import io.streamthoughts.kafka.connect.filepulse.data.TypedValue;
import io.streamthoughts.kafka.connect.filepulse.expression.Expression;
import io.streamthoughts.kafka.connect.filepulse.expression.ExpressionException;
import io.streamthoughts.kafka.connect.filepulse.expression.ValueExpression;
import io.streamthoughts.kafka.connect.filepulse.expression.function.AbstractExpressionFunctionInstance;
import io.streamthoughts.kafka.connect.filepulse.expression.function.Arguments;
import io.streamthoughts.kafka.connect.filepulse.expression.function.EvaluatedExecutionContext;
import io.streamthoughts.kafka.connect.filepulse.expression.function.ExpressionFunction;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

/**
 * An {@link ExpressionFunction} to compute the time difference between two epoch-time in milliseconds.
 */
public class TimestampDiff implements ExpressionFunction {

    /**
     * {@inheritDoc}
     */
    @Override
    public Instance get() {
        return new TimestampDiffInstance();
    }

    public static class TimestampDiffInstance extends AbstractExpressionFunctionInstance {

        private static final String CHRONO_UNIT_ARG = "unit";
        private static final String EPOCH_MILLI_1_ARG = "epoch_millis_expr1";
        private static final String EPOCH_MILLI_2_ARG = "epoch_millis_expr2";

        private ChronoUnit chronoUnit;

        /**
         * {@inheritDoc}
         */
        @Override
        public Arguments prepare(final Expression[] args) throws ExpressionException {
            final String unit = ((ValueExpression) args[0]).value().getString();
            chronoUnit = ChronoUnit.valueOf(unit.toUpperCase());

            return Arguments.of(
                CHRONO_UNIT_ARG, args[0],
                EPOCH_MILLI_1_ARG, args[1],
                EPOCH_MILLI_2_ARG, args[2]
            );
        }

        public TypedValue invoke(final EvaluatedExecutionContext context) throws ExpressionException {
            final Long epochTimeLeft = context.get(1).getLong();
            final Long epochTimeRight = context.get(2).getLong();

            long between = Math.abs(chronoUnit.between(toInstant(epochTimeLeft), toInstant(epochTimeRight)));

            // We need to remove milliseconds precision when one of the two given epoch-timestamps
            // is in seconds but result unit is in milliseconds.
            if (chronoUnit == ChronoUnit.MILLIS &&
               !(isEpochTimeInMillis(epochTimeLeft) && isEpochTimeInMillis(epochTimeRight)) ) {
                between = between < 1000 ? 0 : (between / 1000) * 1000;
            }

            return TypedValue.int64(between);
        }

        /**
         * Converts a given epoch time in seconds or milliseconds to {@link Instant}.
         *
         * @param epochTime the epoch-time to convert.
         * @return          a new {@link Instant}
         */
        @VisibleForTesting
        static Instant toInstant(final long epochTime) {

            if (isEpochTimeInMillis(epochTime)) {
                return Instant.ofEpochMilli(epochTime);
            } else {
                return Instant.ofEpochSecond(epochTime);
            }
        }

        private static boolean isEpochTimeInMillis(long epochTime) {
            // If the given epoch-time has more than 11 digits then
            // it seems to be safe to assume that this timestamp is in milliseconds
            // i.e a the epoch-time represents a date after the Sat Mar 03 1973 09:46:39, otherwise, is in seconds.
            return String.valueOf(epochTime).length() > 11;
        }
    }
}
