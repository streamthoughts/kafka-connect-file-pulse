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
import io.streamthoughts.kafka.connect.filepulse.expression.function.Arguments;
import io.streamthoughts.kafka.connect.filepulse.expression.function.ExecutionContext;
import io.streamthoughts.kafka.connect.filepulse.expression.function.ExpressionFunction;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalField;
import java.time.temporal.TemporalQueries;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

public class ToTimestamp implements ExpressionFunction {

    /**
     * {@inheritDoc}
     */
    @Override
    public Instance get() {
        return new Instance() {

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
            public TypedValue invoke(final ExecutionContext context) throws ExpressionException {
                final String datetime = context.get(0).getString();
                final ZonedDateTime zdt = parser.parse(datetime, zoneId);
                return TypedValue.int64(zdt.toInstant().toEpochMilli());
            }
        };
    }

    static final class DateTimeParser {

        private static final Function<ZoneId, ZonedDateTime> DEFAULT_ZONED_DATE_TIME =
                zid -> ZonedDateTime.of(1970, 1, 1, 0, 0, 0, 0, zid);

        private final DateTimeFormatter formatter;

        /**
         * Creates a new {@link DateTimeParser} instance.
         *
         * @param pattern   the datetime formatter.
         */
        public DateTimeParser(final String pattern) {
            Objects.requireNonNull(pattern, "'pattern' should not be null");
            this.formatter = new DateTimeFormatterBuilder()
                .parseCaseInsensitive()
                .appendPattern(pattern)
                .toFormatter(Locale.ROOT);
        }

        public ZonedDateTime parse(final String datetime, final ZoneId zoneId) {
            final TemporalAccessor parsed = formatter.parse(datetime);

            // Get the target ZoneId from the parsed datetime, or default to the one passed in arguments.
            final ZoneId parsedZoneId = TemporalQueries.zone().queryFrom(parsed);
            final ZoneId atZonedId = Optional.ofNullable(parsedZoneId).orElse(zoneId);

            // Get a new default ZonedDateTime for the ZoneID, then override each temporal field.
            ZonedDateTime resolved = DEFAULT_ZONED_DATE_TIME.apply(atZonedId);
            for (final TemporalField override : ChronoField.values()) {
                if (parsed.isSupported(override)) {
                    final long value = parsed.getLong(override);
                    resolved = resolved.with(override, value);
                }
            }
            return resolved;
        }
    }
}
