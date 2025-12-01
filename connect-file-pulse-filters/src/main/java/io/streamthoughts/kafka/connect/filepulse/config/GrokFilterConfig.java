/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.config;

import io.streamthoughts.kafka.connect.transform.GrokConfig;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;

public class GrokFilterConfig extends CommonFilterConfig {

    private final GrokConfig grok;
    public static final String GROK_FILTER = "GROK_FILTER";
    
    public static final String GROK_TARGET_CONFIG = "target";
    public static final String GROK_TARGET_DOC = "The target field to put the extracted Grok data (optional)";

    /**
     * Creates a new {@link GrokFilterConfig} instance.
     *
     * @param originals the originals configuration.
     */
    public GrokFilterConfig(final Map<String, ?> originals) {
        super(configDef() , originals);
        grok = new GrokConfig(originals);
    }

    public GrokConfig grok() {
        return grok;
    }

    public String target() {
        return getString(GROK_TARGET_CONFIG);
    }

    public static ConfigDef configDef() {
        int filterGroupCounter = 0;
        final ConfigDef def = new ConfigDef(CommonFilterConfig.configDef())
                .define(getSourceConfigKey(GROK_FILTER, filterGroupCounter++))
                .define(getOverwriteConfigKey(GROK_FILTER, filterGroupCounter++))
                .define(
                        GROK_TARGET_CONFIG,
                        ConfigDef.Type.STRING,
                    null,
                        ConfigDef.Importance.HIGH,
                        GROK_TARGET_DOC,
                        GROK_FILTER,
                        filterGroupCounter++,
                        ConfigDef.Width.NONE,
                        GROK_TARGET_CONFIG
                );
        for (ConfigDef.ConfigKey configKey : GrokConfig.configDef().configKeys().values()) {
            def.define(new ConfigDef.ConfigKey(
                    configKey.name,
                    configKey.type,
                    configKey.defaultValue,
                    configKey.validator,
                    configKey.importance,
                    configKey.documentation,
                    GROK_FILTER,
                    filterGroupCounter++,
                    configKey.width,
                    configKey.displayName,
                    configKey.dependents,
                    null,
                    true
            ));
        }
        return def;
    }
}
