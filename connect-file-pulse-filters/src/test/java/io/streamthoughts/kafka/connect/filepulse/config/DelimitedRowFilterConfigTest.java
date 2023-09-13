/*
 * Copyright 2019-2020 StreamThoughts.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.streamthoughts.kafka.connect.filepulse.config;

import static io.streamthoughts.kafka.connect.filepulse.config.DelimitedRowFilterConfig.READER_AUTO_GENERATE_COLUMN_NAME_DEFAULT;
import static io.streamthoughts.kafka.connect.filepulse.config.DelimitedRowFilterConfig.READER_EXTRACT_COLUMN_NAME_CONFIG;
import static io.streamthoughts.kafka.connect.filepulse.config.DelimitedRowFilterConfig.READER_FIELD_COLUMNS_CONFIG;
import static io.streamthoughts.kafka.connect.filepulse.config.DelimitedRowFilterConfig.READER_FIELD_TRIM_COLUMN_CONFIG;
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

}