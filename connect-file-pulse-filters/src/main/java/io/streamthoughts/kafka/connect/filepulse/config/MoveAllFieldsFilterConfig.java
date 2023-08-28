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

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.common.config.ConfigDef;


public class MoveAllFieldsFilterConfig extends CommonFilterConfig {
    public static final String MOVE_TARGET_CONFIG = "target";
    private static final String MOVE_TARGET_DOC = "The target path (support dot-notation)";

    public static final String MOVE_TARGET_CONFIG_DEFAULT = "payload";

    public static final String MOVE_EXCLUDES_CONFIG = "excludes";
    private static final String MOVE_EXCLUDES_DOC = "Commas separated list of fields to exclude";

    public String target() {
        return this.getString(MOVE_TARGET_CONFIG);
    }

    public List<String> excludes() {
        return Optional.ofNullable(this.getString(MOVE_EXCLUDES_CONFIG))
                .stream()
                .flatMap(excludes -> Stream.of(excludes.split(",")))
                .collect(Collectors.toList());
    }

    public static ConfigDef configDef() {
        return new ConfigDef()
                .define(MOVE_TARGET_CONFIG,
                        ConfigDef.Type.STRING,
                        MOVE_TARGET_CONFIG_DEFAULT,
                        ConfigDef.Importance.HIGH,
                        MOVE_TARGET_DOC)
                .define(MOVE_EXCLUDES_CONFIG,
                        ConfigDef.Type.STRING,
                        null,
                        ConfigDef.Importance.LOW,
                        MOVE_EXCLUDES_DOC);
    }

    public MoveAllFieldsFilterConfig(Map<?, ?> originals) {
        super(configDef(), originals);
    }
}
