/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.config;

import io.streamthoughts.kafka.connect.filepulse.source.CompletionPeriod;
import io.streamthoughts.kafka.connect.filepulse.source.DailyCompletionStrategy;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.kafka.common.config.ConfigDef;

/**
 * Configuration for {@link DailyCompletionStrategy}.
 *
 * <p><strong>Deprecated.</strong> Use {@link ScheduledCompletionStrategyConfig} instead,
 * which supports {@code DAILY}, {@code WEEKLY} and {@code MONTHLY} periods.
 *
 * <p>This class accepts the legacy {@code daily.completion.schedule.*} config keys and
 * translates them to the canonical {@code completion.schedule.*} keys understood by
 * {@link ScheduledCompletionStrategyConfig}.
 *
 * <h3>Key mapping</h3>
 * <ul>
 *   <li>{@code daily.completion.schedule.time}         → {@code completion.schedule.time}</li>
 *   <li>{@code daily.completion.schedule.date.pattern} → {@code completion.schedule.date.pattern}</li>
 *   <li>{@code daily.completion.schedule.date.format}  → {@code completion.schedule.date.format}</li>
 * </ul>
 *
 * @deprecated Use {@link ScheduledCompletionStrategyConfig} with {@code completion.schedule.period=DAILY}.
 */
@Deprecated
public class DailyCompletionStrategyConfig extends ScheduledCompletionStrategyConfig {

    /** @deprecated Use {@link ScheduledCompletionStrategyConfig#COMPLETION_SCHEDULE_TIME_CONFIG}. */
    @Deprecated
    public static final String COMPLETION_SCHEDULE_TIME_CONFIG = "daily.completion.schedule.time";

    /** @deprecated Use {@link ScheduledCompletionStrategyConfig#COMPLETION_SCHEDULE_DATE_PATTERN_CONFIG}. */
    @Deprecated
    public static final String COMPLETION_SCHEDULE_DATE_PATTERN_CONFIG = "daily.completion.schedule.date.pattern";

    /** @deprecated Use {@link ScheduledCompletionStrategyConfig#COMPLETION_SCHEDULE_DATE_FORMAT_CONFIG}. */
    @Deprecated
    public static final String COMPLETION_SCHEDULE_DATE_FORMAT_CONFIG = "daily.completion.schedule.date.format";

    /**
     * Creates a new {@link DailyCompletionStrategyConfig} instance.
     *
     * <p>Accepts legacy {@code daily.completion.schedule.*} keys and translates them to the
     * canonical {@code completion.schedule.*} keys before delegating to
     * {@link ScheduledCompletionStrategyConfig}.
     *
     * @param originals the configuration properties.
     */
    public DailyCompletionStrategyConfig(final Map<?, ?> originals) {
        super(translate(originals));
    }

    /** {@inheritDoc} */
    @Override
    public LocalTime scheduledCompletionTime() {
        return super.scheduledCompletionTime();
    }

    /** {@inheritDoc} */
    @Override
    public Pattern datePattern() {
        return super.datePattern();
    }

    /** {@inheritDoc} */
    @Override
    public DateTimeFormatter dateFormatter() {
        return super.dateFormatter();
    }

    /**
     * Returns the {@link ConfigDef} that accepts both the legacy {@code daily.*} keys
     * and the new {@code completion.schedule.*} keys.
     *
     * @return the config definition.
     * @deprecated Use {@link ScheduledCompletionStrategyConfig#configDef()}.
     */
    @Deprecated
    public static ConfigDef configDef() {
        return ScheduledCompletionStrategyConfig.configDef();
    }

    /**
     * Translates a map that may contain legacy {@code daily.completion.schedule.*} keys
     * into a map with the canonical {@code completion.schedule.*} keys.
     *
     * @param originals the original configuration map (may be typed with any key type).
     * @return a new map with translated keys.
     */
    private static Map<String, Object> translate(final Map<?, ?> originals) {
        Map<String, Object> translated = new HashMap<>(originals.size() + 1);

        originals.forEach((rawKey, value) -> {
            String key = rawKey.toString();
            final String mappedKey;
            switch (key) {
                case COMPLETION_SCHEDULE_TIME_CONFIG:
                    mappedKey = ScheduledCompletionStrategyConfig.COMPLETION_SCHEDULE_TIME_CONFIG;
                    break;
                case COMPLETION_SCHEDULE_DATE_PATTERN_CONFIG:
                    mappedKey = ScheduledCompletionStrategyConfig.COMPLETION_SCHEDULE_DATE_PATTERN_CONFIG;
                    break;
                case COMPLETION_SCHEDULE_DATE_FORMAT_CONFIG:
                    mappedKey = ScheduledCompletionStrategyConfig.COMPLETION_SCHEDULE_DATE_FORMAT_CONFIG;
                    break;
                default:
                    mappedKey = key;
            }
            translated.putIfAbsent(mappedKey, value);
        });

        translated.put(ScheduledCompletionStrategyConfig.COMPLETION_SCHEDULE_PERIOD_CONFIG,
            CompletionPeriod.DAILY.name());

        return translated;
    }
}