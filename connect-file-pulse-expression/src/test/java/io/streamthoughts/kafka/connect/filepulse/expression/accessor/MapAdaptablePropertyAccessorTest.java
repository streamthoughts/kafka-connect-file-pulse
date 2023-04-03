/*
 * Copyright 2021 StreamThoughts.
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