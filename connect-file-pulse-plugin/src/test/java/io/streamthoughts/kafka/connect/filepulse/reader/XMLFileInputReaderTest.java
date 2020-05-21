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
package io.streamthoughts.kafka.connect.filepulse.reader;

import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.source.FileContext;
import io.streamthoughts.kafka.connect.filepulse.source.FileRecord;
import io.streamthoughts.kafka.connect.filepulse.source.SourceMetadata;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class XMLFileInputReaderTest {

    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();

    private File file;

    private FileContext context;

    private XMLFileInputReader reader;

    @Before
    public void setUp() throws IOException {
        file = testFolder.newFile();
        try (BufferedWriter bw = Files.newBufferedWriter(file.toPath(), Charset.defaultCharset())) {
            bw.append(XML_DOCUMENT);
            bw.flush();
            context = new FileContext(SourceMetadata.fromFile(file));
        }
        reader = new XMLFileInputReader();
    }

    @Test
    public void shouldReadAllRecordsGivenValidXPathExpression() {
        reader.configure(new HashMap<String, String>(){{
            put(XMLFileInputReaderConfig.XPATH_QUERY_CONFIG, "//broker");
        }});

        FileInputIterator<FileRecord<TypedStruct>> iterator = reader.newIterator(context);
        List<FileRecord<TypedStruct>> records = new ArrayList<>();
        iterator.forEachRemaining(r -> records.addAll(r.collect()));

        Assert.assertEquals(3, records.size());

        assertTopicPartitionObject(records.get(0).value(), "101", "0");
        assertTopicPartitionObject(records.get(1).value(), "102", "1");
        assertTopicPartitionObject(records.get(2).value(), "103", "2");
    }

    @Test
    public void shouldReadAllRecordsGivenRootXPathExpression() {
        reader.configure(new HashMap<String, String>(){{
            put(XMLFileInputReaderConfig.XPATH_QUERY_CONFIG, "/cluster");
        }});

        FileInputIterator<FileRecord<TypedStruct>> iterator = reader.newIterator(context);
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

    private void assertTopicPartitionObject(final TypedStruct struct,
                                            final String expectedId,
                                            final String expectedNum) {
        Assert.assertEquals(expectedId, struct.getString("id"));
        TypedStruct topicPartition = struct.getStruct("topicPartition");
        Assert.assertNotNull(topicPartition);
        Assert.assertEquals("topicA", topicPartition.getString("topic"));
        Assert.assertEquals(expectedNum, topicPartition.getString("num"));
        Assert.assertEquals("true", topicPartition.getString("insync"));
        Assert.assertEquals("0", topicPartition.getString("earliestOffset"));
        Assert.assertEquals("100", topicPartition.getString("endLogOffset"));
        Assert.assertEquals("1G", topicPartition.getString("logSize"));
        Assert.assertEquals("1", topicPartition.getString("numSegments"));
    }

    private static final String XML_DOCUMENT = "" +
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
        "\t</broker>\t\t\n" +
        "</cluster>";
}