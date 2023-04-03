/*
 * Copyright 2023 StreamThoughts.
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
package io.streamthoughts.kafka.connect.filepulse.avro;

import java.io.IOException;
import java.io.InputStream;
import org.apache.kafka.connect.data.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class AvroSchemaConverterTest {

    private final org.apache.avro.Schema.Parser parser = new org.apache.avro.Schema.Parser();

    @Test
    void should_support_avro_circular_reference() throws IOException {
        InputStream inputStream = AvroSchemaConverterTest.class.getClassLoader()
                .getResourceAsStream("datasets/circular.avsc");

        org.apache.avro.Schema avroSchema = parser.parse(inputStream);
        AvroSchemaConverter converter = new AvroSchemaConverter();
        Schema connectSchema = converter.toConnectSchema(avroSchema);
        Assertions.assertNotNull(connectSchema);
    }
}