/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.config;

import io.streamthoughts.kafka.connect.filepulse.internal.LocaleUtils;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;

public class DateFilterConfig extends CommonFilterConfig {

    private static final String GROUP_FILTER_DATE = "DATE_FILTER";

    public static final String DATE_FIELD_CONFIG = "field";
    private static final String DATE_FIELD_DOC = "The field to get the date from.";

    public static final String DATE_TARGET_CONFIG = "target";
    private static final String DATE_TARGET_DOC = "The target field.";

    public static final String DATE_TIMEZONE_CONFIG = "timezone";
    private static final String DATE_TIMEZONE_DOC = "The timezone to use for parsing date.";

    public static final String DATE_LOCALE_CONFIG = "locale";
    private static final String DATE_LOCALE_DOC = "The locale to use for parsing date.";

    public static final String DATE_FORMATS_CONFIG = "formats";
    private static final String DATE_FORMAT_DOC = "List of the expected date formats.";

    /**
     * Creates a new {@link DateFilterConfig} instance.
     *
     * @param originals the configuration.
     */
    public DateFilterConfig(final Map<?, ?> originals) {
        super(configDef(), originals);
    }

    public List<String> formats() {
        return getList(DATE_FORMATS_CONFIG);
    }

    public String field() {
        return getString(DATE_FIELD_CONFIG);
    }

    public String target() {
        return getString(DATE_TARGET_CONFIG);
    }

    public Locale locale() {
        String localeStr = getString(DATE_LOCALE_CONFIG);
        return (localeStr == null) ? Locale.ENGLISH : LocaleUtils.parse(localeStr);
    }

    public ZoneId timezone() {
        String zoneStr = getString(DATE_TIMEZONE_CONFIG);
        return (zoneStr == null) ? ZoneOffset.UTC : ZoneId.of(zoneStr);
    }

    public static ConfigDef configDef() {
        int filterGroupCounter = 0;
        return new ConfigDef(CommonFilterConfig.configDef())
                .define(
                        DATE_TIMEZONE_CONFIG,
                        ConfigDef.Type.STRING,
                        null,
                        ConfigDef.Importance.HIGH,
                        DATE_TIMEZONE_DOC,
                        GROUP_FILTER_DATE,
                        filterGroupCounter++,
                        ConfigDef.Width.NONE,
                        DATE_TIMEZONE_CONFIG
                )
                .define(
                        DATE_LOCALE_CONFIG,
                        ConfigDef.Type.STRING,
                        null,
                        ConfigDef.Importance.HIGH,
                        DATE_LOCALE_DOC,
                        GROUP_FILTER_DATE,
                        filterGroupCounter++,
                        ConfigDef.Width.NONE,
                        DATE_LOCALE_CONFIG
                )
                .define(
                        DATE_FIELD_CONFIG,
                        ConfigDef.Type.STRING,
                        ConfigDef.Importance.HIGH,
                        DATE_FIELD_DOC,
                        GROUP_FILTER_DATE,
                        filterGroupCounter++,
                        ConfigDef.Width.NONE,
                        DATE_FIELD_CONFIG
                )
                .define(
                        DATE_TARGET_CONFIG,
                        ConfigDef.Type.STRING,
                        ConfigDef.Importance.HIGH,
                        DATE_TARGET_DOC,
                        GROUP_FILTER_DATE,
                        filterGroupCounter++,
                        ConfigDef.Width.NONE,
                        DATE_TARGET_CONFIG
                )
                .define(
                        DATE_FORMATS_CONFIG,
                        ConfigDef.Type.LIST,
                        ConfigDef.Importance.HIGH,
                        DATE_FORMAT_DOC,
                        GROUP_FILTER_DATE,
                        filterGroupCounter++,
                        ConfigDef.Width.NONE,
                        DATE_FORMATS_CONFIG
                );

    }
}
