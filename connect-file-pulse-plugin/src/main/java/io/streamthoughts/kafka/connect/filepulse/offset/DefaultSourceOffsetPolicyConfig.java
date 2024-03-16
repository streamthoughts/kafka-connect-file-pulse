/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.offset;

import io.streamthoughts.kafka.connect.filepulse.config.SourceConnectorConfig;
import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

public class DefaultSourceOffsetPolicyConfig extends AbstractConfig {

    public static final String OFFSET_ATTRIBUTES_STRING_CONFIG = "offset.attributes.string";

    private static final String OFFSET_ATTRIBUTES_STRING_DOC = "A separated list of attributes, using '+' character as separator, " +
            "to be used for uniquely identifying an object file; must be one of " +
            "[name, path, lastModified, inode, hash, uri] (e.g: name+hash). Note that order doesn't matter.";

    private static final String OFFSET_ATTRIBUTES_STRING_DEFAULT = "uri";

    /**
     * Creates a new {@link SourceConnectorConfig} instance.
     *
     * @param originals the originals configuration.
     */
    public DefaultSourceOffsetPolicyConfig(final Map<?, ?> originals) {
        super(getConf(), originals);
    }

    public static ConfigDef getConf() {
        return new ConfigDef()
            .define(
                OFFSET_ATTRIBUTES_STRING_CONFIG,
                ConfigDef.Type.STRING,
                OFFSET_ATTRIBUTES_STRING_DEFAULT,
                ConfigDef.Importance.HIGH,
                OFFSET_ATTRIBUTES_STRING_DOC
            );
    }

    public String offsets() {
        return getString(OFFSET_ATTRIBUTES_STRING_CONFIG);
    }
}
