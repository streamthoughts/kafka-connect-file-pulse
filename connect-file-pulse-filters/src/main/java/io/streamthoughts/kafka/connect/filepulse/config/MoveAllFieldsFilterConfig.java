/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
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
