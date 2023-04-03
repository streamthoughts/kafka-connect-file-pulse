/*
 * Copyright 2021 StreamThoughts.
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

import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@code GcsAuthenticationUtils} can be used to build a new {@link Storage} instance from a
 * {@link GcsClientConfig} object.
 */
public class GcsClientUtils {

    private static final Logger LOG = LoggerFactory.getLogger(GcsClientUtils.class);

    /**
     * Helper method to creates a new {@link Storage} object.
     *
     * @param config The Google Cloud Storage configurations
     * @return a new {@link Storage}.
     */
    public static Storage createStorageService(final GcsClientConfig config) {
        final String credentialsPath = config.getCredentialsPath();
        final Password credentialsJsonPwd = config.getCredentialsJson();
        try {
            String credentialsJson = null;
            if (credentialsJsonPwd != null) {
                credentialsJson = credentialsJsonPwd.value();
            }

            if (credentialsPath != null && credentialsJson != null) {
                throw new IllegalArgumentException("Both credentialsPath and credentialsJson cannot be non-null.");
            }

            if (credentialsPath != null) {
                LOG.info("Creating new Google Cloud Storage service using the "
                        + "the credentials file that was passed "
                        + "through the connector's configuration");
                return getStorageServiceForCredentials(
                        getCredentialsFromPath(credentialsPath)
                );
            }

            if (credentialsJson != null) {
                LOG.info("Creating new Google Cloud Storage service using the "
                        + "the credentials JSON that was passed "
                        + "through the connector's configuration");
                return getStorageServiceForCredentials(
                        getCredentialsFromJson(credentialsJson)
                );
            }

            LOG.info("No credentials were passed through the connector's configuration." +
                    " Use default Google Cloud Storage service");
        } catch (final Exception e) {
            throw new ConfigException(
                    "Failed to Google Cloud Storage service using the connector's configuration",
                    e
            );
        }

        return StorageOptions.getDefaultInstance().getService();
    }

    private static Storage getStorageServiceForCredentials(final Credentials credentials) {
        return StorageOptions.newBuilder().setCredentials(credentials).build().getService();
    }

    private static GoogleCredentials getCredentialsFromPath(final String credentialsPath) throws IOException {
        try (final InputStream stream = new FileInputStream(credentialsPath)) {
            return GoogleCredentials.fromStream(stream);
        } catch (final IOException e) {
            LOG.error("Failed to read credentials from JSON string", e);
            throw e;
        }
    }

    private static GoogleCredentials getCredentialsFromJson(final String credentialsJson) throws IOException {
        try (final InputStream stream = new ByteArrayInputStream(credentialsJson.getBytes(StandardCharsets.UTF_8))) {
            return GoogleCredentials.fromStream(stream);
        } catch (final IOException e) {
            LOG.error("Failed to read credentials from JSON string", e);
            throw e;
        }
    }
}
