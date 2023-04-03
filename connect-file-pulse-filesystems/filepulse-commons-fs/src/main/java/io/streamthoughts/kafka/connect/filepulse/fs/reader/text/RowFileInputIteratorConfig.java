/*
 * Copyright 2019-2021 StreamThoughts.
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
package io.streamthoughts.kafka.connect.filepulse.fs.reader.text;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

/**
 * Configuration for reading a file line by line.
 */
public class RowFileInputIteratorConfig extends AbstractConfig {

    public static final String FILE_ENCODING_CONFIG            = "file.encoding";
    public static final String FILE_ENCODING_DOC               = "The text file encoding to use (default = UTF_8)";
    public static final String FILE_ENCODING_DEFAULT           = StandardCharsets.UTF_8.displayName();

    public static final String BUFFER_INIT_BYTES_SIZE_CONFIG   = "buffer.initial.bytes.size";
    public static final String BUFFER_INIT_BYTES_SIZE_DOC      = "The initial buffer size used to read input files";
    public static final int BUFFER_INIT_BYTES_SIZE_DEFAULT     = 4096;

    public static final String MIN_NUM_READ_RECORDS_CONFIG     = "min.read.records";
    public static final String MIN_NUM_READ_RECORDS_DOC        = "The minimum number of records to read from file before returning to task.";

    public static final String READER_FIELD_HEADER_CONFIG      = "skip.headers";
    private static final String READER_FIELD_HEADER_DOC        = "The number of rows to be skipped in the beginning of file.";
    public static final int READER_FIELD_HEADER_DEFAULT        = 0;

    public static final String READER_FIELD_FOOTER_CONFIG      = "skip.footers";
    private static final String READER_FIELD_FOOTER_DOC        = "The number of rows to be skipped at the end of file.";
    public static final int READER_FIELD_FOOTER_DEFAULT        = 0;

    public static final String READER_WAIT_MAX_MS_CONFIG       = "read.max.wait.ms";
    private static final String READER_WAIT_MAX_MS_DOC         = "Maximum time to wait in milliseconds for more bytes after hitting end of file.";
    public static final long READER_WAIT_MAX_MS__DEFAULT       = 0L;

    /**
     * Creates a new {@link RowFileInputIteratorConfig} instance.
     * @param originals the reader configuration.
     */
    public RowFileInputIteratorConfig(final Map<String, ?> originals) {
        super(configDef(), originals);
    }

    public int bufferInitialBytesSize() {
        return getInt(BUFFER_INIT_BYTES_SIZE_CONFIG);
    }

    public int minReadRecords() {
        return getInt(MIN_NUM_READ_RECORDS_CONFIG);
    }

    public Charset charset() {
        return Charset.forName(getString(FILE_ENCODING_CONFIG));
    }

    public int skipHeaders() {
        return getInt(READER_FIELD_HEADER_CONFIG);
    }

    public int skipFooters() { return getInt(READER_FIELD_FOOTER_CONFIG); }

    public long maxWaitMs() { return getLong(READER_WAIT_MAX_MS_CONFIG); }

    private static ConfigDef configDef() {
        return new ConfigDef()
                .define(BUFFER_INIT_BYTES_SIZE_CONFIG, ConfigDef.Type.INT, BUFFER_INIT_BYTES_SIZE_DEFAULT,
                        ConfigDef.Importance.MEDIUM, BUFFER_INIT_BYTES_SIZE_DOC)

                .define(MIN_NUM_READ_RECORDS_CONFIG, ConfigDef.Type.INT, 1,
                        ConfigDef.Importance.MEDIUM, MIN_NUM_READ_RECORDS_DOC)

                .define(FILE_ENCODING_CONFIG, ConfigDef.Type.STRING, FILE_ENCODING_DEFAULT,
                        ConfigDef.Importance.HIGH, FILE_ENCODING_DOC)

                .define(READER_FIELD_HEADER_CONFIG, ConfigDef.Type.INT, READER_FIELD_HEADER_DEFAULT,
                        ConfigDef.Importance.HIGH, READER_FIELD_HEADER_DOC)

                .define(READER_WAIT_MAX_MS_CONFIG, ConfigDef.Type.LONG, READER_WAIT_MAX_MS__DEFAULT,
                        ConfigDef.Importance.LOW, READER_WAIT_MAX_MS_DOC)

                .define(READER_FIELD_FOOTER_CONFIG, ConfigDef.Type.INT, READER_FIELD_FOOTER_DEFAULT,
                        ConfigDef.Importance.HIGH, READER_FIELD_FOOTER_DOC);
    }
}
