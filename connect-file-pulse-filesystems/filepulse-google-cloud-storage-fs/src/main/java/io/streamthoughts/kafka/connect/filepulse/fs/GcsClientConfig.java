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

import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;

/**
 * The Google Cloud Storage client's configuration.
 */
public class GcsClientConfig extends AbstractConfig {

    private static final String GROUP_GCS = "GCS";

    public static final String GCS_CREDENTIALS_PATH_CONFIG = "gcs.credentials.path";

    public static final String GCS_CREDENTIALS_JSON_CONFIG = "gcs.credentials.json";

    private static final String GCS_CREDENTIALS_JSON_DOC = "The GCP credentials as JSON string. "
            + "Cannot be set when \"" + GCS_CREDENTIALS_PATH_CONFIG + "\" is provided. "
            + "If no credentials is specified the client library will look for credentials via "
            + "the environment variable GOOGLE_APPLICATION_CREDENTIALS.";

    private static final String GCS_CREDENTIALS_PATH_DOC = "The path to GCP credentials file. "
            + "Cannot be set when \"" + GCS_CREDENTIALS_JSON_CONFIG + "\" is provided. "
            + "If no credentials is specified the client library will look for credentials via "
            + "the environment variable GOOGLE_APPLICATION_CREDENTIALS.";

    public static final String GCS_BUCKET_NAME_CONFIG = "gcs.bucket.name";
    private static final String GCS_BUCKET_NAME_DOC = "The GCS bucket name to download the object files from.";

    public static final String GCS_BLOBS_FILTER_PREFIX_CONFIG = "gcs.blobs.filter.prefix";
    public static final String GCS_BLOBS_FILTER_PREFIX_DOC = "The prefix to be used for filtering blobs "
            + "whose names begin with it.";

    /**
     * Creates a new {@link GcsClientConfig} instance.
     *
     * @param originals the original configuration map.
     */
    public GcsClientConfig(final Map<?, ?> originals) {
        super(configDef(), originals, false);
        validate();
    }

    private void validate() {
        final Password credentialsJson = getCredentialsJson();
        final String credentialsPath = getCredentialsPath();

        if (credentialsPath != null && credentialsJson != null) {
            throw new ConfigException(String.format(
                    "\"%s\" and \"%s\" are mutually exclusive options, but both are set.",
                    GCS_CREDENTIALS_PATH_CONFIG,
                    GCS_CREDENTIALS_JSON_CONFIG)
            );
        }
    }

    public static ConfigDef configDef() {
        int gscGroupCounter = 0;

        return new ConfigDef()
                .define(
                        GCS_CREDENTIALS_PATH_CONFIG,
                        ConfigDef.Type.STRING,
                        null,
                        ConfigDef.Importance.HIGH,
                        GCS_CREDENTIALS_PATH_DOC,
                        GROUP_GCS,
                        gscGroupCounter++,
                        ConfigDef.Width.NONE,
                        GCS_CREDENTIALS_PATH_CONFIG
                )

                .define(
                        GCS_CREDENTIALS_JSON_CONFIG,
                        ConfigDef.Type.PASSWORD,
                        null,
                        ConfigDef.Importance.HIGH,
                        GCS_CREDENTIALS_JSON_DOC,
                        GROUP_GCS,
                        gscGroupCounter++,
                        ConfigDef.Width.NONE,
                        GCS_CREDENTIALS_JSON_CONFIG
                )

                .define(
                        GCS_BUCKET_NAME_CONFIG,
                        ConfigDef.Type.STRING,
                        ConfigDef.NO_DEFAULT_VALUE,
                        new ConfigDef.NonEmptyString(),
                        ConfigDef.Importance.HIGH,
                        GCS_BUCKET_NAME_DOC,
                        GROUP_GCS,
                        gscGroupCounter++,
                        ConfigDef.Width.NONE,
                        GCS_BUCKET_NAME_CONFIG
                )

                .define(
                        GCS_BLOBS_FILTER_PREFIX_CONFIG,
                        ConfigDef.Type.STRING,
                        null,
                        ConfigDef.Importance.MEDIUM,
                        GCS_BLOBS_FILTER_PREFIX_DOC,
                        GROUP_GCS,
                        gscGroupCounter++,
                        ConfigDef.Width.NONE,
                        GCS_BLOBS_FILTER_PREFIX_CONFIG
                );
    }

    public final String getBlobsPrefix() {
        return getString(GCS_BLOBS_FILTER_PREFIX_CONFIG);
    }

    public final String getCredentialsPath() {
        return getString(GCS_CREDENTIALS_PATH_CONFIG);
    }

    public final Password getCredentialsJson() {
        return getPassword(GCS_CREDENTIALS_JSON_CONFIG);
    }

    public final String getBucketName() {
        return getString(GCS_BUCKET_NAME_CONFIG);
    }
}
