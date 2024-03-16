/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.config;

import io.streamthoughts.kafka.connect.filepulse.MockFileSystemListing;
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
            put(CommonSourceConfig.FS_LISTING_CLASS_CONFIG, MockFileSystemListing.class.getName());
            put(CommonSourceConfig.TASKS_FILE_READER_CLASS_CONFIG, LocalRowFileInputReader.class.getName());
            put(CommonSourceConfig.RECORD_VALUE_SCHEMA_CONFIG, "{\"name\":\"com.example.users.User\",\"type\":\"STRUCT\",\"isOptional\":false,\"fieldSchemas\":{\"id\":{\"type\":\"INT64\",\"isOptional\":false},\"first_name\":{\"type\":\"STRING\",\"isOptional\":true},\"last_name\":{\"type\":\"STRING\",\"isOptional\":true},\"email\":{\"type\":\"STRING\",\"isOptional\":true},\"gender\":{\"type\":\"STRING\",\"isOptional\":true},\"ip_address\":{\"type\":\"STRING\",\"isOptional\":true},\"last_login\":{\"name\":\"org.apache.kafka.connect.data.Timestamp\",\"type\":\"INT64\",\"version\":1,\"isOptional\":true},\"account_balance\":{\"name\":\"org.apache.kafka.connect.data.Decimal\",\"type\":\"BYTES\",\"version\":1,\"parameters\":{\"scale\":\"2\"},\"isOptional\":true},\"country\":{\"type\":\"STRING\",\"isOptional\":true},\"favorite_color\":{\"type\":\"STRING\",\"isOptional\":true}}}");
        }});

        final Schema schema = config.getValueConnectSchema();
        Assert.assertNotNull(schema);
    }

}