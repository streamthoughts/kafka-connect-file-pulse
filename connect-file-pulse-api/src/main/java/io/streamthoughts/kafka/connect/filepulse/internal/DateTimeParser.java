/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.internal;

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

public final class DateTimeParser {

    private static final Function<ZoneId, ZonedDateTime> DEFAULT_ZONED_DATE_TIME =
            zid -> ZonedDateTime.of(1970, 1, 1, 0, 0, 0, 0, zid);

    private final DateTimeFormatter formatter;


    /**
     * Creates a new {@link DateTimeParser} instance.
     *
     * @param pattern   the datetime formatter.
     */
    public DateTimeParser(final String pattern) {
        this(pattern, Locale.ROOT);
    }

    /**
     * Creates a new {@link DateTimeParser} instance.
     *
     * @param pattern   the datetime formatter.
     */
    public DateTimeParser(final String pattern, final Locale locale) {
        Objects.requireNonNull(pattern, "'pattern' should not be null");
        Objects.requireNonNull(pattern, "'locale' should not be null");
        this.formatter = new DateTimeFormatterBuilder()
                .parseCaseInsensitive()
                .appendPattern(pattern)
                .toFormatter(locale);
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
