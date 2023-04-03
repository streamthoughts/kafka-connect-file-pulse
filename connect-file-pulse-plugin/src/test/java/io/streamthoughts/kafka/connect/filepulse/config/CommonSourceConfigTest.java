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
package io.streamthoughts.kafka.connect.filepulse.config;

import io.streamthoughts.kafka.connect.filepulse.fs.reader.LocalRowFileInputReader;
import java.util.HashMap;
import org.apache.kafka.connect.data.Schema;
import org.junit.Assert;
import org.junit.Test;

public class CommonSourceConfigTest {

    @Test
    public void should_configure_record_value_schema() {
        final SourceTaskConfig config = new SourceTaskConfig(new HashMap<>() {{
            put(CommonSourceConfig.OUTPUT_TOPIC_CONFIG, "Test");
            put(CommonSourceConfig.FS_LISTING_CLASS_CONFIG, SourceTaskConfigTest.MockFileSystemListing.class.getName());
            put(CommonSourceConfig.TASKS_FILE_READER_CLASS_CONFIG, LocalRowFileInputReader.class.getName());
            put(CommonSourceConfig.RECORD_VALUE_SCHEMA_CONFIG, "{\"name\":\"com.example.users.User\",\"type\":\"STRUCT\",\"isOptional\":false,\"fieldSchemas\":{\"id\":{\"type\":\"INT64\",\"isOptional\":false},\"first_name\":{\"type\":\"STRING\",\"isOptional\":true},\"last_name\":{\"type\":\"STRING\",\"isOptional\":true},\"email\":{\"type\":\"STRING\",\"isOptional\":true},\"gender\":{\"type\":\"STRING\",\"isOptional\":true},\"ip_address\":{\"type\":\"STRING\",\"isOptional\":true},\"last_login\":{\"name\":\"org.apache.kafka.connect.data.Timestamp\",\"type\":\"INT64\",\"version\":1,\"isOptional\":true},\"account_balance\":{\"name\":\"org.apache.kafka.connect.data.Decimal\",\"type\":\"BYTES\",\"version\":1,\"parameters\":{\"scale\":\"2\"},\"isOptional\":true},\"country\":{\"type\":\"STRING\",\"isOptional\":true},\"favorite_color\":{\"type\":\"STRING\",\"isOptional\":true}}}");
        }});

        final Schema schema = config.getValueConnectSchema();
        Assert.assertNotNull(schema);
    }

}