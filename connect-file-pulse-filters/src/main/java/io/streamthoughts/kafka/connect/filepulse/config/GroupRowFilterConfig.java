/*
 * Copyright 2019 StreamThoughts.
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

import io.streamthoughts.kafka.connect.filepulse.filter.config.CommonFilterConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.List;
import java.util.Map;

public class GroupRowFilterConfig extends CommonFilterConfig {


    public static final String FIELDS_CONFIG                 =   "fields";
    private static final String FIELDS_DOC                   =   "List of fields used to regroup records";

    public static final String MAX_BUFFERED_RECORDS_CONFIG   =   "max.buffered.records";
    private static final String MAX_BUFFERED_RECORDS_DOC     =   "The maximum number of records to group (default : -1).";

    public static final String TARGET_CONFIG                 =   "target";
    private static final String TARGET_DOC                   =   "The target array field to put the grouped field (default : records).";

    /**
     * Creates a new {@link GroupRowFilterConfig} instance.
     *
     * @param originals the originals configs.
     */
    public GroupRowFilterConfig(final Map<?, ?> originals) {
        super(configDef(), originals);
    }

    public String target() {
        return getString(TARGET_CONFIG);
    }

    public List<String> fields() {
        return getList(FIELDS_CONFIG);
    }

    public int maxBufferedRecords() {
        return getInt(MAX_BUFFERED_RECORDS_CONFIG);
    }

    public static ConfigDef configDef() {
        return CommonFilterConfig.configDef()
                .define(FIELDS_CONFIG, ConfigDef.Type.LIST,
                        ConfigDef.Importance.HIGH, FIELDS_DOC)
                .define(MAX_BUFFERED_RECORDS_CONFIG, ConfigDef.Type.INT, -1,
                        ConfigDef.Importance.HIGH, MAX_BUFFERED_RECORDS_DOC)
                .define(TARGET_CONFIG, ConfigDef.Type.STRING, "records",
                        ConfigDef.Importance.HIGH, TARGET_DOC);

    }
}
