/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.fs;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link SmbFileSystemListingConfig}.
 *
 */
public class SmbFileSystemListingConfigTest {

    @Test
    void should_build_valid_config_from_properties() {
        Map<String, Object> props = new HashMap<>();
        props.put(SmbFileSystemListingConfig.SMB_LISTING_HOST_CONFIG, "server");
        props.put(SmbFileSystemListingConfig.SMB_LISTING_SHARE_CONFIG, "share");
        props.put(SmbFileSystemListingConfig.SMB_LISTING_USER_CONFIG, "user");
        props.put(SmbFileSystemListingConfig.SMB_LISTING_PASSWORD_CONFIG, "pass");

        SmbFileSystemListingConfig config = new SmbFileSystemListingConfig(props);

        assertThat(config.getSmbHost()).isEqualTo("server");
        assertThat(config.getSmbShare()).isEqualTo("share");
        assertThat(config.getSmbUser()).isEqualTo("user");
        assertThat(config.getSmbPassword()).isEqualTo("pass");
    }

    @Test
    void configDef_should_define_required_fields() {
        ConfigDef def = SmbFileSystemListingConfig.configDef();

        assertThat(def.configKeys())
                .containsKeys(
                        SmbFileSystemListingConfig.SMB_LISTING_HOST_CONFIG,
                        SmbFileSystemListingConfig.SMB_LISTING_SHARE_CONFIG,
                        SmbFileSystemListingConfig.SMB_LISTING_USER_CONFIG,
                        SmbFileSystemListingConfig.SMB_LISTING_PASSWORD_CONFIG
                );
    }
}
