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
package io.streamthoughts.kafka.connect.filepulse.xml;

import io.streamthoughts.kafka.connect.filepulse.data.FieldPaths;
import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

public class XMLNodeToStructConverterTest {

    private final XMLDocumentReader reader = new XMLDocumentReader(true, true);

    @Test
    public void should_convert_given_xml_document_with_comment_type_node() throws IOException, SAXException {
        final byte[] bytes = "<!-- THIS IS A COMMENT--><root attr=\"text\">test</root>".getBytes();
        final XMLNodeToStructConverter converter = new XMLNodeToStructConverter();
        final Document document = reader.parse(new ByteArrayInputStream(bytes));
        Assert.assertNotNull(converter.apply(document));
    }

    @Test
    public void should_convert_given_xml_document_with_document_type_node() throws IOException, SAXException {
        final byte[] bytes = "<!DOCTYPE DATA><root attr=\"text\">test</root>".getBytes();
        final XMLNodeToStructConverter converter = new XMLNodeToStructConverter();
        final Document document = reader.parse(new ByteArrayInputStream(bytes));
        Assert.assertNotNull(converter.apply(document));
    }

    @Test
    public void should_prefix_node_attributes() throws IOException, SAXException {

        // Given
        final XMLNodeToStructConverter converter = new XMLNodeToStructConverter()
                .setAttributePrefix("prefix_");

        // When
        final Document document = reader.parse(new ByteArrayInputStream("<root attr=\"text\">test</root>".getBytes()));
        final TypedStruct result = converter.apply(document);

        // Then
        final String path = "root.prefix_attr";
        Assert.assertTrue(result.exists(path));
        Assert.assertEquals("text", result.find(path).getString());
    }

    @Test
    public void should_not_prefix_node_attributes() throws IOException, SAXException {
        // Given
        final XMLNodeToStructConverter converter = new XMLNodeToStructConverter();

        // When
        final Document document = reader.parse(new ByteArrayInputStream("<root attr=\"text\">test</root>".getBytes()));
        final TypedStruct result = converter.apply(document);

        // Then
        final String path = "root.attr";
        Assert.assertTrue(result.exists(path));
        Assert.assertEquals("text", result.find(path).getString());
    }

    @Test
    public void should_convert_given_single_text_node_element_without_attr() throws Exception {
        // Given
        final byte[] bytes = "<root>test</root>".getBytes();
        final XMLNodeToStructConverter converter = new XMLNodeToStructConverter()
                .setContentFieldName("text");

        // When
        final Document document = reader.parse(new ByteArrayInputStream(bytes));
        TypedStruct struct = converter.apply(document);

        // Then
        Assert.assertNotNull(struct);
        Assert.assertEquals("test", struct.getString("root"));
    }

    @Test
    public void should_convert_given_single_text_node_element_without_attr_and_force_content() throws Exception {
        // Given
        final byte[] bytes = "<root>test</root>".getBytes();
        final XMLNodeToStructConverter converter = new XMLNodeToStructConverter()
                .setContentFieldName("text")
                .setForceContentFields(FieldPaths.from(List.of("root")));

        // When
        final Document document = reader.parse(new ByteArrayInputStream(bytes));
        TypedStruct struct = converter.apply(document);

        // Then
        Assert.assertNotNull(struct);
        TypedStruct root = struct.getStruct("root");
        Assert.assertEquals("test", root.getString("text"));
    }

    @Test
    public void should_convert_given_single_text_node_element_with_attrs() throws Exception {
        // Given
        final byte[] bytes = "<root attr=\"attr\">test</root>".getBytes();
        final XMLNodeToStructConverter converter = new XMLNodeToStructConverter()
                .setContentFieldName("text");

        // When
        final Document document = reader.parse(new ByteArrayInputStream(bytes));
        TypedStruct struct = converter.apply(document);

        // Then
        Assert.assertNotNull(struct);
        TypedStruct root = struct.getStruct("root");
        Assert.assertEquals("test", root.getString("text"));
        Assert.assertEquals("attr", root.getString("attr"));
    }

    @Test
    public void should_ignore_element_given_xml_tag_with_whitespaces() throws Exception {
        // Given
        final byte[] bytes = "<root><empty>            </empty><data>text</data></root>".getBytes();
        final XMLNodeToStructConverter converter = new XMLNodeToStructConverter()
                .setContentFieldName("text")
                .setExcludeEmptyElement(true);

        // When
        final Document document = reader.parse(new ByteArrayInputStream(bytes));
        TypedStruct struct = converter.apply(document);

        // Then
        Assert.assertNotNull(struct);
        TypedStruct root = struct.getStruct("root");
        Assert.assertNotNull(root);
        Assert.assertFalse(root.has("empty"));
    }

    @Test
    public void should_ignore_empty_element_given_self_closing_xml_tag() throws Exception {
        // Given
        final byte[] bytes = "<root><empty/><data>text</data></root>".getBytes();
        final XMLNodeToStructConverter converter = new XMLNodeToStructConverter()
                .setContentFieldName("text")
                .setExcludeEmptyElement(true);

        // When
        final Document document = reader.parse(new ByteArrayInputStream(bytes));
        TypedStruct struct = converter.apply(document);

        // Then
        Assert.assertNotNull(struct);
        TypedStruct root = struct.getStruct("root");
        Assert.assertNotNull(root);
        Assert.assertFalse(root.has("empty"));
    }

    @Test
    public void should_ignore_empty_element_given_open_and_close_xml_tags() throws Exception {
        // Given
        final byte[] bytes = "<root><empty></empty><data>text</data></root>".getBytes();
        final XMLNodeToStructConverter converter = new XMLNodeToStructConverter()
                .setContentFieldName("text")
                .setExcludeEmptyElement(true);

        // When
        final Document document = reader.parse(new ByteArrayInputStream(bytes));
        TypedStruct struct = converter.apply(document);

        // Then
        Assert.assertNotNull(struct);
        TypedStruct root = struct.getStruct("root");
        Assert.assertNotNull(root);
        Assert.assertFalse(root.has("empty"));
    }
}