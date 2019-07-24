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

import io.streamthoughts.kafka.connect.filepulse.config.ConvertFilterConfig;
import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.source.FileRecordOffset;
import io.streamthoughts.kafka.connect.filepulse.source.SourceMetadata;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConvertFilterTest {

    private ConvertFilter filter;

    private FilterContext context;

    private Map<String, String> configs;

    @Before
    public void setUp() {
        filter = new ConvertFilter();
        configs = new HashMap<>();
        context = FilterContextBuilder.newBuilder()
                .withMetadata(new SourceMetadata("", "", 0L, 0L, 0L, -1L))
                .withOffset(FileRecordOffset.empty())
                .build();
    }

    @Test
    public void test() {
        configs.put(ConvertFilterConfig.CONVERT_FIELD_CONFIG, "foo");
        configs.put(ConvertFilterConfig.CONVERT_TYPE_CONFIG, "boolean");
        filter.configure(configs);

        TypedStruct struct = new TypedStruct().put("foo", "yes");
        List<TypedStruct> results = filter.apply(context, struct, false).collect();

        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());

        TypedStruct result = results.get(0);
        Assert.assertTrue(result.getBoolean("foo"));
    }
}