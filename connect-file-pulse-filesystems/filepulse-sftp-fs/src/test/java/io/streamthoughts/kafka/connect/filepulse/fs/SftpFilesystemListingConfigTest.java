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
package io.streamthoughts.kafka.connect.filepulse.fs;

import static io.streamthoughts.kafka.connect.filepulse.fs.SftpFilesystemListingConfig.CONNECTOR_NAME_PROPERTY;
import static io.streamthoughts.kafka.connect.filepulse.fs.SftpFilesystemListingConfig.SFTP_CONNECTION_TIMEOUT_MS;
import static io.streamthoughts.kafka.connect.filepulse.fs.SftpFilesystemListingConfig.SFTP_LISTING_DIRECTORY_PATH;
import static io.streamthoughts.kafka.connect.filepulse.fs.SftpFilesystemListingConfig.SFTP_LISTING_HOST;
import static io.streamthoughts.kafka.connect.filepulse.fs.SftpFilesystemListingConfig.SFTP_LISTING_PASSWORD;
import static io.streamthoughts.kafka.connect.filepulse.fs.SftpFilesystemListingConfig.SFTP_LISTING_PORT;
import static io.streamthoughts.kafka.connect.filepulse.fs.SftpFilesystemListingConfig.SFTP_LISTING_USER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import io.streamthoughts.kafka.connect.filepulse.errors.ConnectFilePulseException;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class SftpFilesystemListingConfigTest {

    private static Stream<Arguments> when_config_initialized_getSftpListingDirectoryPath_should_return_the_path() {
        return Stream.of(arguments("/test/path", "/test/path"),
                arguments("/test/path/", "/test/path"));
    }

    @ParameterizedTest
    @MethodSource
    void when_config_initialized_getSftpListingDirectoryPath_should_return_the_path(String path, String expectedPath) {

        SftpFilesystemListingConfig config =
                new SftpFilesystemListingConfig(Map.of(
                        SFTP_LISTING_HOST, "h",
                        SFTP_LISTING_USER, "u",
                        SFTP_LISTING_PASSWORD, "p",
                        SFTP_LISTING_DIRECTORY_PATH, path));

        assertEquals(expectedPath, config.getSftpListingDirectoryPath());
    }

    @Test
    void when_hostname_not_specified_config_initialization_should_throw_exception() {
        assertThrows(ConfigException.class, () -> new SftpFilesystemListingConfig(Map.of(
                        SFTP_LISTING_USER, "u",
                        SFTP_LISTING_PASSWORD, "p",
                        SFTP_LISTING_DIRECTORY_PATH, "/path")));
    }

    @Test
    void when_username_not_specified_config_initialization_should_throw_exception() {
        assertThrows(ConfigException.class, () -> new SftpFilesystemListingConfig(Map.of(
                SFTP_LISTING_HOST, "h",
                SFTP_LISTING_PASSWORD, "p",
                SFTP_LISTING_DIRECTORY_PATH, "/path")));
    }

    @Test
    void when_password_not_specified_config_initialization_should_throw_exception() {
        assertThrows(ConfigException.class, () -> new SftpFilesystemListingConfig(Map.of(
                SFTP_LISTING_HOST, "h",
                SFTP_LISTING_USER, "u",
                SFTP_LISTING_DIRECTORY_PATH, "/path")));
    }

    @Test
    void when_root_path_not_specified_config_initialization_should_throw_exception() {
        assertThrows(ConfigException.class, () -> new SftpFilesystemListingConfig(Map.of(
                SFTP_LISTING_HOST, "h",
                SFTP_LISTING_USER, "u",
                SFTP_LISTING_PASSWORD, "p")));
    }

    @Test
    void when_port_not_specified_config_initialization_should_assign_default_sftp_port() {
        SftpFilesystemListingConfig config =
                new SftpFilesystemListingConfig(Map.of(
                        SFTP_LISTING_HOST, "h",
                        SFTP_LISTING_USER, "u",
                        SFTP_LISTING_PASSWORD, "p",
                        SFTP_LISTING_DIRECTORY_PATH, "/path"));

        assertEquals(22, config.getSftpListingPort());
    }

    @Test
    void when_port_specified_config_initialization_should_override_default_sftp_port() {
        SftpFilesystemListingConfig config =
                new SftpFilesystemListingConfig(Map.of(
                        SFTP_LISTING_HOST, "h",
                        SFTP_LISTING_USER, "u",
                        SFTP_LISTING_PORT, 1234,
                        SFTP_LISTING_PASSWORD, "p",
                        SFTP_LISTING_DIRECTORY_PATH, "/path"));

        assertEquals(1234, config.getSftpListingPort());
    }

    @Test
    void when_connection_timeout_not_specified_config_initialization_should_assign_default_timeout() {
        SftpFilesystemListingConfig config =
                new SftpFilesystemListingConfig(Map.of(
                        SFTP_LISTING_HOST, "h",
                        SFTP_LISTING_USER, "u",
                        SFTP_LISTING_PASSWORD, "p",
                        SFTP_LISTING_DIRECTORY_PATH, "/path"));

        assertEquals(10000, config.getSftpConnectionTimeoutMs());
    }

    @Test
    void when_connection_timeout_specified_config_initialization_should_override_default_timeout() {
        SftpFilesystemListingConfig config =
                new SftpFilesystemListingConfig(Map.of(
                        SFTP_LISTING_HOST, "h",
                        SFTP_LISTING_USER, "u",
                        SFTP_CONNECTION_TIMEOUT_MS, 5000,
                        SFTP_LISTING_PASSWORD, "p",
                        SFTP_LISTING_DIRECTORY_PATH, "/path"));

        assertEquals(5000, config.getSftpConnectionTimeoutMs());
    }

    @Test
    void when_config_has_name_property_getConnectorName_should_return_the_name() {
        SftpFilesystemListingConfig config =
                new SftpFilesystemListingConfig(Map.of(
                        SFTP_LISTING_HOST, "h",
                        SFTP_LISTING_USER, "u",
                        SFTP_LISTING_PASSWORD, "p",
                        SFTP_LISTING_DIRECTORY_PATH, "/path",
                        CONNECTOR_NAME_PROPERTY, "test_connector"));

        assertEquals("test_connector", config.getConnectorName());
    }

    @Test
    void when_config_does_not_have_name_property_getConnectorName_should_throw_exception() {
        SftpFilesystemListingConfig config =
                new SftpFilesystemListingConfig(Map.of(
                        SFTP_LISTING_HOST, "h0",
                        SFTP_LISTING_USER, "u",
                        SFTP_LISTING_PASSWORD, "p",
                        SFTP_LISTING_DIRECTORY_PATH, "/path"));

        assertThrows(ConnectFilePulseException.class, config::getConnectorName);
    }

}