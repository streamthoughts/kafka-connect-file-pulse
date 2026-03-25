/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.source;

import io.streamthoughts.kafka.connect.filepulse.config.DailyCompletionStrategyConfig;
import io.streamthoughts.kafka.connect.filepulse.config.ScheduledCompletionStrategyConfig;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link FileCompletionStrategy} that marks files as COMPLETED the <strong>day after</strong>
 * the date extracted from the filename, at a configurable time of day.
 *
 * <p><strong>Deprecated.</strong> Use {@link ScheduledCompletionStrategy} with
 * {@code completion.schedule.period=DAILY} instead. This class is kept for backward compatibility
 * and simply delegates to {@link ScheduledCompletionStrategy} after translating the legacy
 * {@code daily.completion.schedule.*} config keys to the new {@code completion.schedule.*} keys.
 *
 * <h3>Legacy configuration keys (still supported)</h3>
 * <ul>
 *   <li>{@code daily.completion.schedule.time}         → {@code completion.schedule.time}</li>
 *   <li>{@code daily.completion.schedule.date.pattern} → {@code completion.schedule.date.pattern}</li>
 *   <li>{@code daily.completion.schedule.date.format}  → {@code completion.schedule.date.format}</li>
 * </ul>
 *
 * @deprecated Use {@link ScheduledCompletionStrategy} with {@code completion.schedule.period=DAILY}.
 */
@Deprecated
public class DailyCompletionStrategy extends ScheduledCompletionStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(DailyCompletionStrategy.class);

    /**
     * {@inheritDoc}
     *
     * <p>Translates the legacy {@code daily.completion.schedule.*} config keys to the new
     * {@code completion.schedule.*} keys
     */
    @Override
    public void configure(final Map<String, ?> configs) {
        LOG.warn(
            "DailyCompletionStrategy is deprecated. " +
                "Please migrate to ScheduledCompletionStrategy with completion.schedule.period=DAILY."
        );

        Map<String, Object> translated = new HashMap<>(configs.size() + 1);

        // Copy all existing keys first (allows completion.schedule.* keys to be passed directly
        // if someone is already partially migrated)
        configs.forEach((k, v) -> translated.put(k.toString(), v));

        // Remap legacy daily.* keys → completion.schedule.* keys (only if the new key is not already set)
        remapIfAbsent(configs, translated,
            DailyCompletionStrategyConfig.COMPLETION_SCHEDULE_TIME_CONFIG,
            ScheduledCompletionStrategyConfig.COMPLETION_SCHEDULE_TIME_CONFIG);

        remapIfAbsent(configs, translated,
            DailyCompletionStrategyConfig.COMPLETION_SCHEDULE_DATE_PATTERN_CONFIG,
            ScheduledCompletionStrategyConfig.COMPLETION_SCHEDULE_DATE_PATTERN_CONFIG);

        remapIfAbsent(configs, translated,
            DailyCompletionStrategyConfig.COMPLETION_SCHEDULE_DATE_FORMAT_CONFIG,
            ScheduledCompletionStrategyConfig.COMPLETION_SCHEDULE_DATE_FORMAT_CONFIG);

        // Always force DAILY period regardless of what caller may have set
        translated.put(ScheduledCompletionStrategyConfig.COMPLETION_SCHEDULE_PERIOD_CONFIG,
            CompletionPeriod.DAILY.name());

        super.configure(translated);
    }

    /**
     * Copies the value from {@code legacyKey} in {@code source} into {@code target} under
     * {@code newKey}, but only when {@code newKey} is not already present in {@code target}.
     */
    private static void remapIfAbsent(
        final Map<String, ?> source,
        final Map<String, Object> target,
        final String legacyKey,
        final String newKey
    ) {
        if (!target.containsKey(newKey) && source.containsKey(legacyKey)) {
            target.put(newKey, source.get(legacyKey));
        }
    }
}