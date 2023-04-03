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
package io.streamthoughts.kafka.connect.filepulse.expression.accessor;

import io.streamthoughts.kafka.connect.filepulse.data.TypedValue;
import io.streamthoughts.kafka.connect.filepulse.expression.StandardEvaluationContext;
import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

public class ReflectivePropertyAccessorTest {

    private final StandardEvaluationContext context = new StandardEvaluationContext(new Object());

    @Test(expected = AccessException.class)
    public void should_thrown_when_writing_invalid_property_using_getter_method_given_pojo() {
        ReflectivePropertyAccessor accessor = new ReflectivePropertyAccessor();
        accessor.write(context, new DummyObject("foo"), "unknown", "");
    }

    @Test(expected = AccessException.class)
    public void should_thrown_when_reading_invalid_property_using_getter_method_given_pojo() {
        ReflectivePropertyAccessor accessor = new ReflectivePropertyAccessor();
        accessor.read(context, new DummyObject("foo"), "unknown");
    }

    @Test
    public void should_write_property_using_setter_method_given_pojo_and_expected_parameter() {
        ReflectivePropertyAccessor accessor = new ReflectivePropertyAccessor();
        DummyObject object = new DummyObject(null);
        accessor.write(context, object, "value", "foo");
        Assert.assertEquals("foo", object.value);
    }

    @Test
    public void should_write_null_property_using_setter_method_given_pojo_and_expected_parameter() {
        ReflectivePropertyAccessor accessor = new ReflectivePropertyAccessor();
        DummyObject object = new DummyObject("foo");
        accessor.write(context, object, "value", null);
        Assert.assertNull(object.value);
    }

    @Test
    public void should_write_property_using_setter_method_given_pojo() {
        ReflectivePropertyAccessor accessor = new ReflectivePropertyAccessor();
        DummyObject object = new DummyObject(null);
        accessor.write(context, object, "value", TypedValue.string("foo"));
        Assert.assertEquals("foo", object.value);
    }

    @Test
    public void should_read_property_using_getter_method_given_pojo() {
        ReflectivePropertyAccessor accessor = new ReflectivePropertyAccessor();
        Object object = accessor.read(context, new DummyObject("foo"), "value");
        Assert.assertEquals("foo", object);
    }

    @Test
    public void should_read_property_given_dotted_path() {
        ReflectivePropertyAccessor accessor = new ReflectivePropertyAccessor();
        Map<String, String> map = new HashMap<>();
        map.put("key", "foo");
        Object object = accessor.read(context, new DummyObject(map), "value.key");
        Assert.assertEquals("foo", object);
    }

    public static class DummyObject {

        private Object value;

        DummyObject(Object value) {
            this.value = value;
        }

        public Object getValue() {
            return value;
        }

        public void setValue(Object value) {
            this.value = value;
        }
    }
}
