/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
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