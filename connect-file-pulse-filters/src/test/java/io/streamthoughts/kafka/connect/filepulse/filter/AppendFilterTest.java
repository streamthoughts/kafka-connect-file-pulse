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

import io.streamthoughts.kafka.connect.filepulse.config.AppendFilterConfig;
import io.streamthoughts.kafka.connect.filepulse.source.SourceMetadata;
import io.streamthoughts.kafka.connect.filepulse.reader.RecordsIterable;
import io.streamthoughts.kafka.connect.filepulse.source.FileInputData;
import io.streamthoughts.kafka.connect.filepulse.source.FileInputOffset;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AppendFilterTest {

    private AppendFilter filter;

    private FilterContext context;

    private Map<String, String> configs;

    @Before
    public void setUp() {
        filter = new AppendFilter();
        configs = new HashMap<>();
        SourceMetadata metadata = new SourceMetadata("", "", 0L, 0L, 0L, -1L);
        context = InternalFilterContext.with(metadata, FileInputOffset.empty());
    }

    @Test
    public void testGivenSubstitutionExpression() {
        configs.put(AppendFilterConfig.APPEND_FIELD_CONFIG, "target");
        configs.put(AppendFilterConfig.APPEND_VALUE_CONFIG, "{{ extract_array(values,0) }}-{{ extract_array(values,1) }}");
        filter.configure(configs);

        List<String> values = Arrays.asList("foo", "bar");
        context.set("values", new SchemaAndValue(SchemaBuilder.array(SchemaBuilder.string()), values));
        RecordsIterable<FileInputData> output = filter.apply(context, null);
        Assert.assertNotNull(output);
        FileInputData data = output.collect().get(0);
        Assert.assertEquals("foo-bar", data.value().getString("target"));
    }
}