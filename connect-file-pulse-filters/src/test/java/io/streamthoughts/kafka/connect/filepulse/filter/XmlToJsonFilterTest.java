/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
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