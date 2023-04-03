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
package io.streamthoughts.kafka.connect.filepulse.filter;

import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import org.junit.Assert;
import org.junit.Test;

public class XmlToJsonFilterTest {

    String XML = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
            "<root>\n" +
            "  <element1 attr=\"foo\">bar</element1>\n" +
            "  <element2>value</element2>\n" +
            "  <element3>value</element3>\n" +
            "  <element4>value1</element4>\n" +
            "  <element4>value2</element4>\n" +
            "  <element5></element5>\n" +
            "</root>";

    @Test
    public void should_success_to_convert_xml_to_json_given_input_string() {
        final XmlToJsonFilter filter = new XmlToJsonFilter();
        filter.configure(Collections.emptyMap());
        final TypedStruct input = TypedStruct.create().put("message", XML);
        final TypedStruct output = filter.apply(null, input).iterator().next();

        Assert.assertNotNull(output);
        Assert.assertEquals(
            "{\"root\":{\"element1\":{\"attr\":\"foo\",\"value\":\"bar\"},\"element2\":\"value\",\"element3\":\"value\",\"element4\":[\"value1\",\"value2\"],\"element5\":\"\"}}",
            output.getString("message")
        );
    }

    @Test
    public void should_success_to_convert_xml_to_json_given_input_bytes() {
        final XmlToJsonFilter filter = new XmlToJsonFilter();
        filter.configure(Collections.emptyMap());
        final TypedStruct input = TypedStruct.create().put("message", XML.getBytes(StandardCharsets.UTF_8));
        final TypedStruct output = filter.apply(null, input).iterator().next();

        Assert.assertNotNull(output);
        Assert.assertEquals(
                "{\"root\":{\"element1\":{\"attr\":\"foo\",\"value\":\"bar\"},\"element2\":\"value\",\"element3\":\"value\",\"element4\":[\"value1\",\"value2\"],\"element5\":\"\"}}",
                output.getString("message")
        );
    }
}