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
package io.streamthoughts.kafka.connect.filepulse.config;

import java.util.Map;
import java.util.regex.Pattern;
import org.apache.kafka.common.config.ConfigDef;

public class ExcludeFieldsMatchingPatternsConfig extends CommonFilterConfig {
    public static final String EXCLUDE_FIELDS_REGEX_CONFIG = "regex";

    private static final String EXCLUDE_FIELDS_REGEX_CONFIG_DOC = "Regexp pattern applied to a field value to determine if the fields should be propagated or not.";

    public static final String EXCLUDE_FIELDS_BLOCK_FIELD_CONFIG = "block.field";

    private static final String EXCLUDE_FIELDS_BLOCK_FIELD_CONFIG_DOC = "If true, omits propagating the field downstream. Otherwise, propagates the field with a null value";

    public ExcludeFieldsMatchingPatternsConfig(Map<?, ?> originals) {
        super(configDef(), originals);
    }

    public static ConfigDef configDef() {
        return new ConfigDef()
                .define(EXCLUDE_FIELDS_REGEX_CONFIG,
                        ConfigDef.Type.STRING,
                        ConfigDef.Importance.HIGH,
                        EXCLUDE_FIELDS_REGEX_CONFIG_DOC)
                .define(EXCLUDE_FIELDS_BLOCK_FIELD_CONFIG,
                        ConfigDef.Type.BOOLEAN,
                        false,
                        ConfigDef.Importance.HIGH,
                        EXCLUDE_FIELDS_BLOCK_FIELD_CONFIG_DOC);
    }

    public Pattern pattern() {
        return Pattern.compile(this.getString(EXCLUDE_FIELDS_REGEX_CONFIG));
    }

    public boolean blockField() {
        return this.getBoolean(EXCLUDE_FIELDS_BLOCK_FIELD_CONFIG);
    }

}
