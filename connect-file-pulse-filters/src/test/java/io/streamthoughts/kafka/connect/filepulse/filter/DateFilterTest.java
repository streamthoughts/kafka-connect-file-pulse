/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.filter;

import io.streamthoughts.kafka.connect.filepulse.config.DateFilterConfig;
import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.source.FileRecordOffset;
import io.streamthoughts.kafka.connect.filepulse.source.GenericFileObjectMeta;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DateFilterTest {

    private DateFilter filter;
    private FilterContext context;
    private Map<String, Object> configs;

    @BeforeEach
    public void setUp() {
        filter = new DateFilter();
        configs = new HashMap<>();
        context = FilterContextBuilder.newBuilder()
                .withMetadata(new GenericFileObjectMeta(null, "", 0L, 0L, null, null))
                .withOffset(FileRecordOffset.invalid())
                .build();
    }

    @Test
    public void shouldConvertToEpochTimeGivenDate() {
        configs.put(DateFilterConfig.DATE_FIELD_CONFIG, "$.date");
        configs.put(DateFilterConfig.DATE_TARGET_CONFIG, "$.timestamp");
        configs.put(DateFilterConfig.DATE_FORMATS_CONFIG, Collections.singletonList("yyyy-MM-dd"));

        filter.configure(configs, alias -> null);
        TypedStruct struct = TypedStruct.create().put("date", "2001-07-04");
        List<TypedStruct> results = filter.apply(context, struct, false).collect();

        TypedStruct record = results.get(0);

        Assertions.assertEquals(994204800000L, record.getLong("timestamp").longValue());
    }

    @Test
    public void shouldConvertToEpochTimeGivenNoTimezoneAndNoLocale() {
        configs.put(DateFilterConfig.DATE_FIELD_CONFIG, "$.date");
        configs.put(DateFilterConfig.DATE_TARGET_CONFIG, "$.timestamp");
        configs.put(DateFilterConfig.DATE_FORMATS_CONFIG, Collections.singletonList("yyyy-MM-dd'T'HH:mm:ss"));

        filter.configure(configs, alias -> null);
        TypedStruct struct = TypedStruct.create().put("date", "2001-07-04T12:08:56");
        List<TypedStruct> results = filter.apply(context, struct, false).collect();

        TypedStruct record = results.get(0);

        Assertions.assertEquals(994248536000L, record.getLong("timestamp").longValue());
    }

    @Test
    public void shouldConvertToEpochTimeGivenTimezone() {
        configs.put(DateFilterConfig.DATE_FIELD_CONFIG, "$.date");
        configs.put(DateFilterConfig.DATE_TARGET_CONFIG, "$.timestamp");
        configs.put(DateFilterConfig.DATE_TIMEZONE_CONFIG, "Europe/Paris");
        configs.put(DateFilterConfig.DATE_FORMATS_CONFIG, Collections.singletonList("yyyy-MM-dd'T'HH:mm:ss"));

        filter.configure(configs, alias -> null);
        TypedStruct struct = TypedStruct.create().put("date", "2001-07-04T14:08:56");
        List<TypedStruct> results = filter.apply(context, struct, false).collect();

        TypedStruct record = results.get(0);

        Assertions.assertEquals(994248536000L, record.getLong("timestamp").longValue());
    }

    @Test
    public void shouldConvertToEpochTimeGivenLocale() {
        configs.put(DateFilterConfig.DATE_FIELD_CONFIG, "$.date");
        configs.put(DateFilterConfig.DATE_TARGET_CONFIG, "$.timestamp");
        configs.put(DateFilterConfig.DATE_LOCALE_CONFIG, "fr_FR");
        configs.put(DateFilterConfig.DATE_FORMATS_CONFIG, Collections.singletonList("EEEE, d MMMM yyyy HH:mm:ss"));

        filter.configure(configs, alias -> null);
        TypedStruct struct = TypedStruct.create().put("date", "mercredi, 4 juillet 2001 12:08:56");
        List<TypedStruct> results = filter.apply(context, struct, false).collect();

        TypedStruct record = results.get(0);

        Assertions.assertEquals(994248536000L, record.getLong("timestamp").longValue());
    }

}