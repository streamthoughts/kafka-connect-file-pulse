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
package io.streamthoughts.kafka.connect.filepulse.filter;

import io.streamthoughts.kafka.connect.filepulse.config.DateFilterConfig;
import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.internal.LocaleUtils;
import io.streamthoughts.kafka.connect.filepulse.source.FileRecordOffset;
import io.streamthoughts.kafka.connect.filepulse.source.SourceMetadata;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;

import static org.junit.Assert.*;

public class DateFilterTest {

    private DateFilter filter;
    private FilterContext context;
    private Map<String, Object> configs;

    @Before
    public void setUp() {
        filter = new DateFilter();
        configs = new HashMap<>();
        context = FilterContextBuilder.newBuilder()
                .withMetadata(new SourceMetadata("", "", 0L, 0L, 0L, -1L))
                .withOffset(FileRecordOffset.empty())
                .build();
    }

    @Test
    public void shouldConvertToEpochTimeGivenNoTimezoneAndNoLocale() {
        configs.put(DateFilterConfig.DATE_FIELD_CONFIG, "date");
        configs.put(DateFilterConfig.DATE_TARGET_CONFIG, "timestamp");
        configs.put(DateFilterConfig.DATE_FORMATS_CONFIG, Collections.singletonList("yyyy-MM-dd'T'HH:mm:ss"));

        filter.configure(configs);
        TypedStruct struct = new TypedStruct().put("date", "2001-07-04T12:08:56");
        List<TypedStruct> results = filter.apply(context, struct, false).collect();

        TypedStruct record = results.get(0);

        Assert.assertEquals(994248536000L, record.getLong("timestamp").longValue());
    }

    @Test
    public void shouldConvertToEpochTimeGivenTimezone() {
        configs.put(DateFilterConfig.DATE_FIELD_CONFIG, "date");
        configs.put(DateFilterConfig.DATE_TARGET_CONFIG, "timestamp");
        configs.put(DateFilterConfig.DATE_TIMEZONE_CONFIG, "Europe/Paris");
        configs.put(DateFilterConfig.DATE_FORMATS_CONFIG, Collections.singletonList("yyyy-MM-dd'T'HH:mm:ss"));

        filter.configure(configs);
        TypedStruct struct = new TypedStruct().put("date", "2001-07-04T14:08:56");
        List<TypedStruct> results = filter.apply(context, struct, false).collect();

        TypedStruct record = results.get(0);

        Assert.assertEquals(994248536000L, record.getLong("timestamp").longValue());
    }

    @Test
    public void shouldConvertToEpochTimeGivenLocale() {
        configs.put(DateFilterConfig.DATE_FIELD_CONFIG, "date");
        configs.put(DateFilterConfig.DATE_TARGET_CONFIG, "timestamp");
        configs.put(DateFilterConfig.DATE_LOCALE_CONFIG, "fr_FR");
        configs.put(DateFilterConfig.DATE_FORMATS_CONFIG, Collections.singletonList("EEEE, d MMMM yyyy HH:mm:ss"));

        filter.configure(configs);
        TypedStruct struct = new TypedStruct().put("date", "mercredi, 4 juillet 2001 12:08:56");
        List<TypedStruct> results = filter.apply(context, struct, false).collect();

        TypedStruct record = results.get(0);

        Assert.assertEquals(994248536000L, record.getLong("timestamp").longValue());
    }

}