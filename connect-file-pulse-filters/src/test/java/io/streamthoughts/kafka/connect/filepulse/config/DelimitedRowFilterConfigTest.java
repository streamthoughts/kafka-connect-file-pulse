/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.config;

import static io.streamthoughts.kafka.connect.filepulse.config.DelimitedRowFilterConfig.READER_AUTO_GENERATE_COLUMN_NAME_CONFIG_ALIAS;
import static io.streamthoughts.kafka.connect.filepulse.config.DelimitedRowFilterConfig.READER_AUTO_GENERATE_COLUMN_NAME_DEFAULT;
import static io.streamthoughts.kafka.connect.filepulse.config.DelimitedRowFilterConfig.READER_EXTRACT_COLUMN_NAME_CONFIG;
import static io.streamthoughts.kafka.connect.filepulse.config.DelimitedRowFilterConfig.READER_EXTRACT_COLUMN_NAME_CONFIG_ALIAS;
import static io.streamthoughts.kafka.connect.filepulse.config.DelimitedRowFilterConfig.READER_FIELD_COLUMNS_CONFIG;
import static io.streamthoughts.kafka.connect.filepulse.config.DelimitedRowFilterConfig.READER_FIELD_DUPLICATE_COLUMNS_AS_ARRAY_CONFIG;
import static io.streamthoughts.kafka.connect.filepulse.config.DelimitedRowFilterConfig.READER_FIELD_DUPLICATE_COLUMNS_AS_ARRAY_CONFIG_ALIAS;
import static io.streamthoughts.kafka.connect.filepulse.config.DelimitedRowFilterConfig.READER_FIELD_TRIM_COLUMN_CONFIG;
import static io.streamthoughts.kafka.connect.filepulse.config.DelimitedRowFilterConfig.READER_FIELD_TRIM_COLUMN_CONFIG_ALIAS;
import static io.streamthoughts.kafka.connect.filepulse.config.DelimitedRowFilterConfig.READER_FIELD_TRIM_COLUMN_DEFAULT;
import static io.streamthoughts.kafka.connect.filepulse.filter.DelimitedRowFilter.READER_FIELD_SEPARATOR_CONFIG;
import static io.streamthoughts.kafka.connect.filepulse.filter.DelimitedRowFilter.READER_FIELD_SEPARATOR_DEFAULT;

import io.streamthoughts.kafka.connect.filepulse.data.StructSchema;
import io.streamthoughts.kafka.connect.filepulse.data.TypedField;
import io.streamthoughts.kafka.connect.filepulse.filter.DelimitedRowFilter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

public class DelimitedRowFilterConfigTest {

    private final DelimitedRowFilter filter = new DelimitedRowFilter();

    @Test
    public void shouldCreateConfigWithDefaultValues() {
        DelimitedRowFilterConfig config = new DelimitedRowFilterConfig(filter.configDef(), new HashMap<>());
        Assert.assertEquals(READER_FIELD_SEPARATOR_DEFAULT, config.getString(READER_FIELD_SEPARATOR_CONFIG));
        Assert.assertNull(config.extractColumnName());
        Assert.assertEquals(READER_FIELD_TRIM_COLUMN_DEFAULT, config.isTrimColumn());
        Assert.assertEquals(READER_AUTO_GENERATE_COLUMN_NAME_DEFAULT, config.isAutoGenerateColumnNames());
        Assert.assertNull(config.schema());
    }

    @Test
    public void shouldCreateConfigGivenOverrideValues() {

        Map<String, String> props = new HashMap<>() {{
            put(READER_FIELD_SEPARATOR_CONFIG, "|");
            put(READER_EXTRACT_COLUMN_NAME_CONFIG, "header");
            put(READER_FIELD_TRIM_COLUMN_CONFIG, "true");
        }};

        DelimitedRowFilterConfig config = new DelimitedRowFilterConfig(filter.configDef(), props);

        Assert.assertEquals("|", config.getString(READER_FIELD_SEPARATOR_CONFIG));
        Assert.assertEquals("header", config.extractColumnName());
        Assert.assertTrue(config.isTrimColumn());
        Assert.assertNull(config.schema());
    }

    @Test
    public void shouldHonourDeprecatedAliasEvenWhenNewKeyHasDefault() {
        // Reproduces the bug: when a framework pre-fills new key with its ConfigDef default (false),
        // the deprecated alias (trimColumn=true) must still take effect.
        Map<String, Object> props = new HashMap<>() {{
            put(READER_FIELD_TRIM_COLUMN_CONFIG, false);           // default pre-filled by framework
            put(READER_FIELD_TRIM_COLUMN_CONFIG_ALIAS, true);      // user explicitly set deprecated alias
            put(READER_AUTO_GENERATE_COLUMN_NAME_CONFIG_ALIAS, true);
        }};

        Map<String, Object> translated = CommonFilterConfig.translateDeprecatedConfigs(props, deprecatedAliasGroups());
        Assert.assertFalse("Deprecated alias key must not remain in translated config map",
                translated.containsKey(READER_FIELD_TRIM_COLUMN_CONFIG_ALIAS));
        Assert.assertEquals(true, translated.get(READER_FIELD_TRIM_COLUMN_CONFIG));

        DelimitedRowFilterConfig config = new DelimitedRowFilterConfig(filter.configDef(), props);

        Assert.assertTrue("Deprecated alias trimColumn=true must override the pre-filled default trim.column=false",
                config.isTrimColumn());
    }

    @Test
    public void shouldRemoveDeprecatedAliasesFromTranslatedConfigMap() {
        Map<String, Object> props = new HashMap<>() {{
            put(READER_FIELD_TRIM_COLUMN_CONFIG_ALIAS, true);
            put(READER_FIELD_DUPLICATE_COLUMNS_AS_ARRAY_CONFIG_ALIAS, true);
            put(READER_EXTRACT_COLUMN_NAME_CONFIG_ALIAS, "header");
            put(READER_AUTO_GENERATE_COLUMN_NAME_CONFIG_ALIAS, false);
            put("custom.config", "custom-value");
        }};

        Map<String, Object> translated = CommonFilterConfig.translateDeprecatedConfigs(props, deprecatedAliasGroups());

        Assert.assertFalse(translated.containsKey(READER_FIELD_TRIM_COLUMN_CONFIG_ALIAS));
        Assert.assertFalse(translated.containsKey(READER_FIELD_DUPLICATE_COLUMNS_AS_ARRAY_CONFIG_ALIAS));
        Assert.assertFalse(translated.containsKey(READER_EXTRACT_COLUMN_NAME_CONFIG_ALIAS));
        Assert.assertFalse(translated.containsKey(READER_AUTO_GENERATE_COLUMN_NAME_CONFIG_ALIAS));

        Assert.assertEquals(true, translated.get(READER_FIELD_TRIM_COLUMN_CONFIG));
        Assert.assertEquals(true, translated.get(READER_FIELD_DUPLICATE_COLUMNS_AS_ARRAY_CONFIG));
        Assert.assertEquals("header", translated.get(READER_EXTRACT_COLUMN_NAME_CONFIG));
        Assert.assertEquals(false, translated.get(DelimitedRowFilterConfig.READER_AUTO_GENERATE_COLUMN_NAME_CONFIG));
        Assert.assertEquals("custom-value", translated.get("custom.config"));
    }

    @Test
    public void shouldCreateConfigGivenSchema() {

        Map<String, String> props = new HashMap<>() {{
            put(READER_FIELD_COLUMNS_CONFIG, "x1:BOOLEAN;c2:INT32;y3:STRING");
        }};

        DelimitedRowFilterConfig config = new DelimitedRowFilterConfig(filter.configDef(), props);

        StructSchema schema = config.schema();
        Assert.assertNotNull(schema);

        // Fields ordered by name (default)
        List<TypedField> fields = schema.fields();
        Assert.assertEquals(3, fields.size());

        Assert.assertEquals("c2", fields.get(0).name());
        Assert.assertEquals("x1", fields.get(1).name());
        Assert.assertEquals("y3", fields.get(2).name());

        // Fields ordered by index
        List<TypedField> fieldsByIndex = schema.fieldsByIndex();
        Assert.assertEquals(3, fieldsByIndex.size());

        Assert.assertEquals("x1", fieldsByIndex.get(0).name());
        Assert.assertEquals("c2", fieldsByIndex.get(1).name());
        Assert.assertEquals("y3", fieldsByIndex.get(2).name());
    }

    private static Map<String, List<String>> deprecatedAliasGroups() {
        return Map.of(
                READER_FIELD_TRIM_COLUMN_CONFIG, List.of(READER_FIELD_TRIM_COLUMN_CONFIG_ALIAS),
                READER_FIELD_DUPLICATE_COLUMNS_AS_ARRAY_CONFIG, List.of(READER_FIELD_DUPLICATE_COLUMNS_AS_ARRAY_CONFIG_ALIAS),
                READER_EXTRACT_COLUMN_NAME_CONFIG, List.of(READER_EXTRACT_COLUMN_NAME_CONFIG_ALIAS),
                DelimitedRowFilterConfig.READER_AUTO_GENERATE_COLUMN_NAME_CONFIG, List.of(READER_AUTO_GENERATE_COLUMN_NAME_CONFIG_ALIAS)
        );
    }

}