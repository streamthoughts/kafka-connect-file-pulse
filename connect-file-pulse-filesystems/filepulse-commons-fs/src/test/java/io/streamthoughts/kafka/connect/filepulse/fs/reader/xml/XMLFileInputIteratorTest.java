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
package io.streamthoughts.kafka.connect.filepulse.fs.reader.xml;

import static io.streamthoughts.kafka.connect.filepulse.fs.reader.xml.XMLFileInputReaderConfig.withKeyPrefix;

import io.streamthoughts.kafka.connect.filepulse.data.Type;
import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.data.TypedValue;
import io.streamthoughts.kafka.connect.filepulse.fs.reader.IteratorManager;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectMeta;
import io.streamthoughts.kafka.connect.filepulse.source.FileRecord;
import io.streamthoughts.kafka.connect.filepulse.source.LocalFileObjectMeta;
import io.streamthoughts.kafka.connect.filepulse.source.TypedFileRecord;
import io.streamthoughts.kafka.connect.filepulse.xml.XMLCommonConfig;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class XMLFileInputIteratorTest {

    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();

    @Test
    public void should_read_all_records_given_valid_xpath_expression() throws IOException {
        final XMLFileInputReaderConfig config = new XMLFileInputReaderConfig(Collections.singletonMap(
                XMLFileInputReaderConfig.XPATH_QUERY_CONFIG, "//broker")
        );

        try (
                XMLFileInputIterator iterator = createXMLFileInputIterator(config, DEFAULT_TEST_XML_DOCUMENT)
        ) {
            List<FileRecord<TypedStruct>> records = new ArrayList<>();
            iterator.forEachRemaining(r -> records.addAll(r.collect()));

            Assert.assertEquals(3, records.size());

            assertTopicPartitionObject(records.get(0).value(), "101", "0");
            assertTopicPartitionObject(records.get(1).value(), "102", "1");
            assertTopicPartitionObject(records.get(2).value(), "103", "2");
        }
    }

    @Test
    public void should_read_all_records_given_root_xpath_expression() throws IOException {
        final XMLFileInputReaderConfig config = new XMLFileInputReaderConfig(Collections.singletonMap(
                XMLFileInputReaderConfig.XPATH_QUERY_CONFIG, "/cluster")
        );

        try (
                XMLFileInputIterator iterator = createXMLFileInputIterator(config, DEFAULT_TEST_XML_DOCUMENT)
        ) {
            List<FileRecord<TypedStruct>> records = new ArrayList<>();
            iterator.forEachRemaining(r -> records.addAll(r.collect()));

            Assert.assertEquals(1, records.size());

            FileRecord<TypedStruct> record = records.get(0);
            TypedStruct struct = record.value();
            Assert.assertEquals("my-cluster", struct.getString("id"));
            Assert.assertEquals("2.3.0", struct.getString("version"));
            List<TypedStruct> brokers = struct.getArray("broker");
            Assert.assertEquals(3, brokers.size());

            assertTopicPartitionObject(brokers.get(0), "101", "0");
            assertTopicPartitionObject(brokers.get(1), "102", "1");
            assertTopicPartitionObject(brokers.get(2), "103", "2");
        }

    }

    @Test
    public void should_ignore_white_space_and_nl_nodes() throws IOException {

        final XMLFileInputReaderConfig config = new XMLFileInputReaderConfig(Collections.singletonMap(
                XMLFileInputReaderConfig.XPATH_QUERY_CONFIG, "/")
        );

        try (
                XMLFileInputIterator iterator1 = createXMLFileInputIterator(config, "<ROOT><DATA>data</DATA></ROOT>");
                XMLFileInputIterator iterator2 = createXMLFileInputIterator(config, "<ROOT>\n\t<DATA>data</DATA>\n</ROOT>")
        ) {
            FileRecord<TypedStruct> rs1 = iterator1.next().last();
            FileRecord<TypedStruct> rs2 = iterator2.next().last();
            Assert.assertEquals(rs1.value(), rs2.value());
        }
    }

    @Test
    public void should_read_record_given_document_with_cdata_node() throws IOException {

        final XMLFileInputReaderConfig config = new XMLFileInputReaderConfig(Collections.singletonMap(
               XMLFileInputReaderConfig.XPATH_QUERY_CONFIG, "/")
        );

        try (var iterator = createXMLFileInputIterator(config, CDATA_TEST_XML_DOCUMENT)) {
            List<FileRecord<TypedStruct>> records = new ArrayList<>();
            iterator.forEachRemaining(r -> records.addAll(r.collect()));

            Assert.assertEquals(1, records.size());
            Assert.assertEquals("dummy text", records.get(0).value().getString("ROOT"));
        }
    }

    @Test
    public void should_read_record_given_document_with_comment_node() throws IOException {

        final XMLFileInputReaderConfig config = new XMLFileInputReaderConfig(Collections.singletonMap(
               XMLFileInputReaderConfig.XPATH_QUERY_CONFIG, "/")
        );

        try (var iterator = createXMLFileInputIterator(config, COMMENT_TEST_XML_DOCUMENT)) {
            List<FileRecord<TypedStruct>> records = new ArrayList<>();
            iterator.forEachRemaining(r -> records.addAll(r.collect()));

            Assert.assertEquals(1, records.size());
            Assert.assertEquals("dummy text", records.get(0).value().getString("ROOT"));
        }
    }

    @Test
    public void should_read_record_given_node_xpath_expression() throws IOException {
        final XMLFileInputReaderConfig config = new XMLFileInputReaderConfig(new HashMap<String, String>() {{
            put(XMLFileInputReaderConfig.XPATH_QUERY_CONFIG, "(//broker)[1]/topicPartition/logSize/text()");
            put(XMLFileInputReaderConfig.XPATH_RESULT_TYPE_CONFIG, "STRING");
        }});

        try (var iterator = createXMLFileInputIterator(config, DEFAULT_TEST_XML_DOCUMENT)) {
            List<FileRecord<TypedStruct>> records = new ArrayList<>();
            iterator.forEachRemaining(r -> records.addAll(r.collect()));

            Assert.assertEquals(1, records.size());
            Assert.assertEquals("1G", records.get(0).value().getString(TypedFileRecord.DEFAULT_MESSAGE_FIELD));
        }
    }

    @Test
    public void should_read_record_given_valid_force_array_fields() throws IOException {
        final XMLFileInputReaderConfig config = new XMLFileInputReaderConfig(new HashMap<String, String>() {{
            put(XMLFileInputReaderConfig.XPATH_QUERY_CONFIG, "//broker[1]");
            put(withKeyPrefix(XMLFileInputReaderConfig.XML_FORCE_ARRAY_ON_FIELDS_CONFIG), "broker.topicPartition, broker.topicPartition.numSegments");
        }});

        try (var iterator = createXMLFileInputIterator(config, DEFAULT_TEST_XML_DOCUMENT)) {
            List<FileRecord<TypedStruct>> records = new ArrayList<>();
            iterator.forEachRemaining(r -> records.addAll(r.collect()));

            Assert.assertEquals(1, records.size());

            TypedStruct record = records.get(0).value();
            Assert.assertEquals(Type.ARRAY, record.get("topicPartition").type());
            Assert.assertEquals(1, record.get("topicPartition").getArray().size());
            TypedValue segment = ((TypedStruct) record.get("topicPartition").getArray().iterator().next()).find("numSegments");
            Assert.assertEquals(Type.ARRAY, segment.type());
            Assert.assertEquals(1, segment.getArray().size());
        }
    }

    @Test
    public void should_read_record_given_valid_force_array_fields_and_default_xpath() throws IOException {
        final XMLFileInputReaderConfig config = new XMLFileInputReaderConfig(new HashMap<String, String>() {{
            put(withKeyPrefix(XMLCommonConfig.XML_FORCE_ARRAY_ON_FIELDS_CONFIG), "cluster.broker.topicPartition");
        }});

        try (var iterator = createXMLFileInputIterator(config, DEFAULT_TEST_XML_DOCUMENT)) {
            List<FileRecord<TypedStruct>> records = new ArrayList<>();
            iterator.forEachRemaining(r -> records.addAll(r.collect()));

            TypedStruct record = records.get(0).value();
            TypedStruct element = (TypedStruct) record.find("cluster.broker").getArray().iterator().next();
            Assert.assertEquals(Type.ARRAY, element.find("topicPartition").type());
        }
    }

    @Test
    public void should_read_record_given_single_text_node_with_attrs() throws IOException {
        final XMLFileInputReaderConfig config = new XMLFileInputReaderConfig(Collections.emptyMap());
        try (var iterator = createXMLFileInputIterator(config, TEXT_NODE_TEST_XML_DOCUMENT)) {
            List<FileRecord<TypedStruct>> records = new ArrayList<>();
            iterator.forEachRemaining(r -> records.addAll(r.collect()));

            Assert.assertEquals(1, records.size());
            Assert.assertEquals("dummy text", records.get(0).value().find("ROOT.value").getString());
            Assert.assertEquals("dummy attr", records.get(0).value().find("ROOT.attr").getString());
        }
    }

    private XMLFileInputIterator createXMLFileInputIterator(final XMLFileInputReaderConfig config,
                                                            final String xmlDocument) throws IOException {
        File file = testFolder.newFile();
        FileObjectMeta meta;
        try (BufferedWriter bw = Files.newBufferedWriter(file.toPath(), Charset.defaultCharset())) {
            bw.append(xmlDocument);
            bw.flush();
            meta = new LocalFileObjectMeta(file);
        }
        return new XMLFileInputIterator(config, new IteratorManager(), meta, new FileInputStream(file));
    }

    private static void assertTopicPartitionObject(final TypedStruct struct,
                                                   final String expectedId,
                                                   final String expectedNum) {
        Assert.assertEquals(expectedId, struct.getString("id"));

        TypedStruct topicPartition = struct.first("topicPartition").getStruct();
        Assert.assertNotNull(topicPartition);
        Assert.assertEquals("topicA", topicPartition.getString("topic"));
        Assert.assertEquals(expectedNum, topicPartition.getString("num"));
        Assert.assertEquals("true", topicPartition.getString("insync"));
        Assert.assertEquals("0", topicPartition.getString("earliestOffset"));
        Assert.assertEquals("100", topicPartition.getString("endLogOffset"));
        Assert.assertEquals("1G", topicPartition.getString("logSize"));
        Assert.assertEquals("1", topicPartition.getString("numSegments"));
    }

    private static final String TEXT_NODE_TEST_XML_DOCUMENT = "<ROOT attr=\"dummy attr\">dummy text</ROOT>";

    private static final String COMMENT_TEST_XML_DOCUMENT = "<ROOT><!-- This is a comment -->dummy text</ROOT>";

    private static final String CDATA_TEST_XML_DOCUMENT = "<ROOT>\n\t<![CDATA[dummy text]]>\n</ROOT>";

    private static final String DEFAULT_TEST_XML_DOCUMENT = "" +
            "<cluster id=\"my-cluster\" version=\"2.3.0\">\n" +
            "\t<broker id=\"101\">\n" +
            "\t\t<topicPartition topic=\"topicA\" num=\"0\" insync=\"true\">\n" +
            "\t\t\t<earliestOffset>0</earliestOffset>\n" +
            "\t\t\t<endLogOffset>100</endLogOffset>\n" +
            "\t\t\t<logSize>1G</logSize>\n" +
            "\t\t\t<numSegments>1</numSegments>\n" +
            "\t\t</topicPartition>\n" +
            "\t</broker>\n" +
            "\t<broker id=\"102\">\n" +
            "\t\t<topicPartition topic=\"topicA\" num=\"1\" insync=\"true\">\n" +
            "\t\t\t<earliestOffset>0</earliestOffset>\n" +
            "\t\t\t<endLogOffset>100</endLogOffset>\n" +
            "\t\t\t<logSize>1G</logSize>\n" +
            "\t\t\t<numSegments>1</numSegments>\n" +
            "\t\t</topicPartition>\n" +
            "\t</broker>\n" +
            "\t<broker id=\"103\">\n" +
            "\t\t<topicPartition topic=\"topicA\" num=\"2\" insync=\"true\">\n" +
            "\t\t\t<earliestOffset>0</earliestOffset>\n" +
            "\t\t\t<endLogOffset>100</endLogOffset>\n" +
            "\t\t\t<logSize>1G</logSize>\n" +
            "\t\t\t<numSegments>1</numSegments>\n" +
            "\t\t</topicPartition>\n" +
            "\t\t<topicPartition topic=\"topicA\" num=\"3\" insync=\"true\">\n" +
            "\t\t\t<earliestOffset>0</earliestOffset>\n" +
            "\t\t\t<endLogOffset>100</endLogOffset>\n" +
            "\t\t\t<logSize>1G</logSize>\n" +
            "\t\t\t<numSegments>1</numSegments>\n" +
            "\t\t</topicPartition>\n" +
            "\t</broker>\t\t\n" +
            "</cluster>";
}