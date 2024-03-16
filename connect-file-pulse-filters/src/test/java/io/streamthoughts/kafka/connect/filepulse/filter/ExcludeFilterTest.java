/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
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