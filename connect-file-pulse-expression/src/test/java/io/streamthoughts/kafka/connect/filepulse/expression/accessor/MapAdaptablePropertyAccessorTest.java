/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.expression.accessor;


import io.streamthoughts.kafka.connect.filepulse.expression.StandardEvaluationContext;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

public class MapAdaptablePropertyAccessorTest {

    private final StandardEvaluationContext context = new StandardEvaluationContext(new Object());

    @Test
    public void should_read_property_from_map_given_simple_key() {
        MapAdaptablePropertyAccessor accessor = new MapAdaptablePropertyAccessor();
        Object value = accessor.read(context, Map.of("key", "value"), "key");
        Assert.assertEquals("value", value);
    }

    @Test
    public void should_read_property_from_map_given_simple_keyeee() {
        MapAdaptablePropertyAccessor accessor = new MapAdaptablePropertyAccessor();
        Object value = accessor.read(context, Map.of("key", "value"), "key");
        Assert.assertEquals("value", value);
    }

    @Test
    public void should_read_property_from_map_given_dotted_key() {
        MapAdaptablePropertyAccessor accessor = new MapAdaptablePropertyAccessor();
        Object value = accessor.read(context,   Map.of("key1", Map.of("key2", "value")), "key1.key2");
        Assert.assertEquals("value", value);
    }
}