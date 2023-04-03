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

import io.streamthoughts.kafka.connect.filepulse.config.ExcludeFilterConfig;
import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.reader.RecordsIterable;
import java.util.HashMap;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ExcludeFilterTest {

    private final TypedStruct struct = TypedStruct.create()
            .insert("fieldOne", "1")
            .insert("fieldTwo", "2")
            .insert("fieldThree.fieldFour", "4")
            .insert("fieldThree.fieldFive", "5");

    private ExcludeFilter filter;

    @Before
    public void setUp() {
        filter = new ExcludeFilter();

    }

    @Test
    public void should_exclude_given_existing_fields() {
        filter.configure(new HashMap<String, String>() {{
            put(ExcludeFilterConfig.EXCLUDE_FIELDS_CONFIG, "fieldOne, fieldTwo");
        }});

        RecordsIterable<TypedStruct> result = filter.apply(null, struct, false);
        Assert.assertEquals(1, result.size());
        Assert.assertFalse(result.last().exists("fieldOne"));
        Assert.assertFalse(result.last().exists("fieldTwo"));
        Assert.assertTrue(result.last().exists("fieldThree"));
    }

    @Test
    public void should_exclude_given_existing_path() {
        filter.configure(new HashMap<String, String>() {{
            put(ExcludeFilterConfig.EXCLUDE_FIELDS_CONFIG, "fieldThree.fieldFive");
        }});

        RecordsIterable<TypedStruct> result = filter.apply(null, struct, false);
        Assert.assertEquals(1, result.size());
        Assert.assertTrue(result.last().exists("fieldOne"));
        Assert.assertTrue(result.last().exists("fieldTwo"));
        Assert.assertTrue(result.last().exists("fieldThree"));
        Assert.assertTrue(result.last().exists("fieldThree.fieldFour"));
        Assert.assertFalse(result.last().exists("fieldThree.fieldFive"));
    }

    @Test
    public void should_ignore_unknown_fields() {
        filter.configure(new HashMap<String, String>() {{
            put(ExcludeFilterConfig.EXCLUDE_FIELDS_CONFIG, "fieldFour");
        }});

        RecordsIterable<TypedStruct> result = filter.apply(null, struct, false);
        Assert.assertEquals(1, result.size());
        Assert.assertTrue(result.last().exists("fieldOne"));
        Assert.assertTrue(result.last().exists("fieldTwo"));
        Assert.assertTrue(result.last().exists("fieldThree"));
    }
}