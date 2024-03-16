/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
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
