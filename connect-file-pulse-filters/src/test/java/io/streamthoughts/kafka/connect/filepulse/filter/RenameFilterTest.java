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

import io.streamthoughts.kafka.connect.filepulse.config.RenameFilterConfig;
import io.streamthoughts.kafka.connect.filepulse.source.SourceMetadata;
import io.streamthoughts.kafka.connect.filepulse.source.FileInputData;
import io.streamthoughts.kafka.connect.filepulse.source.FileInputOffset;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class RenameFilterTest {

    private RenameFilter filter;

    private FilterContext context;

    private Map<String, String> configs;

    @Before
    public void setUp() {
        filter = new RenameFilter();
        configs = new HashMap<>();
        SourceMetadata metadata = new SourceMetadata("", "", 0L, 0L, 0L, -1L);
        context = InternalFilterContext.with(metadata, FileInputOffset.empty());
    }

    @Test
    public void shouldRenameGivenExistingField() {

        configs.put(RenameFilterConfig.RENAME_FIELD_CONFIG, "foo");
        configs.put(RenameFilterConfig.RENAME_TARGET_CONFIG, "bar");
        filter.configure(configs);

        SchemaBuilder schema = SchemaBuilder.struct().field("foo", SchemaBuilder.string());
        Struct struct = new Struct(schema).put("foo", "dummy-value");
        FileInputData record = new FileInputData(struct);
        List<Struct> results = this.filter.apply(null, record, false)
                .stream()
                .map(o -> (Struct) o.value())
                .collect(Collectors.toList());

        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());

        Struct result = results.get(0);
        Assert.assertEquals("dummy-value", result.getString("bar"));
    }
}