/*
 * Copyright 2019-2020 StreamThoughts.
 *
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
import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class RenameFilterTest {

    private RenameFilter filter;

    private Map<String, String> configs;

    @Before
    public void setUp() {
        filter = new RenameFilter();
        configs = new HashMap<>();
    }

    @Test
    public void should_rename_given_existing_fields() {

        configs.put(RenameFilterConfig.RENAME_FIELD_CONFIG, "foo");
        configs.put(RenameFilterConfig.RENAME_TARGET_CONFIG, "bar");
        filter.configure(configs, alias -> null);

        TypedStruct record = TypedStruct.create().put("foo", "dummy-value");
        List<TypedStruct> results = filter.apply(null, record, false).collect();

        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());

        TypedStruct result = results.get(0);
        Assert.assertEquals("dummy-value", result.getString("bar"));
    }

    @Test
    public void should_rename_given_existing_path() {

        configs.put(RenameFilterConfig.RENAME_FIELD_CONFIG, "foo.bar");
        configs.put(RenameFilterConfig.RENAME_TARGET_CONFIG, "foo");
        filter.configure(configs, alias -> null);

        TypedStruct record = TypedStruct.create().insert("foo.bar", "dummy-value");
        List<TypedStruct> results = filter.apply(null, record, false).collect();

        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());

        TypedStruct result = results.get(0);
        Assert.assertEquals("dummy-value", result.find("foo.foo").getString());
    }
}