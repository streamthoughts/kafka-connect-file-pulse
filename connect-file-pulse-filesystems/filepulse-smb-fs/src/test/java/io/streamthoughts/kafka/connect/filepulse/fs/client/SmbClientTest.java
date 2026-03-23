/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.fs.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.streamthoughts.kafka.connect.filepulse.fs.SmbFileSystemListingConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SmbClientTest {

    private SmbClient smbClient;

    @BeforeEach
    void setUp() {
        SmbFileSystemListingConfig config = mock(SmbFileSystemListingConfig.class);
        // Needed by buildBaseUrl()
        when(config.getSmbHost()).thenReturn("samba");
        when(config.getSmbShare()).thenReturn("kafka-sink");
        // Needed by createCifsContext()
        when(config.getSmbMaxVersion()).thenReturn("SMB311");
        when(config.getResponseTimeout()).thenReturn(35000);
        when(config.getConnectionTimeout()).thenReturn(35000);
        when(config.getSoTimeout()).thenReturn(35000);
        when(config.getSmbDomain()).thenReturn("WORKGROUP");
        when(config.getSmbUser()).thenReturn("user");
        when(config.getSmbPassword()).thenReturn("password");

        smbClient = new SmbClient(config);
    }

    @Test
    void buildDirectoryUrl_should_return_base_url_when_directory_path_is_root() {
        assertThat(smbClient.buildDirectoryUrl("/"))
                .isEqualTo("smb://samba/kafka-sink/");
    }

    @Test
    void buildDirectoryUrl_should_return_base_url_when_directory_path_is_empty() {
        assertThat(smbClient.buildDirectoryUrl(""))
                .isEqualTo("smb://samba/kafka-sink/");
    }

    @Test
    void buildDirectoryUrl_should_handle_nested_subdirectory_without_trailing_slash() {
        assertThat(smbClient.buildDirectoryUrl("/input/orders/2025"))
            .isEqualTo("smb://samba/kafka-sink/input/orders/2025/");
    }

    @Test
    void buildDirectoryUrl_should_handle_nested_subdirectory() {
        assertThat(smbClient.buildDirectoryUrl("/input/orders/2025/"))
            .isEqualTo("smb://samba/kafka-sink/input/orders/2025/");
    }

    @Test
    void buildDirectoryUrl_should_handle_path_without_leading_and_trailing_slash() {
        assertThat(smbClient.buildDirectoryUrl("input/orders/2025"))
                .isEqualTo("smb://samba/kafka-sink/input/orders/2025/");
    }
}
