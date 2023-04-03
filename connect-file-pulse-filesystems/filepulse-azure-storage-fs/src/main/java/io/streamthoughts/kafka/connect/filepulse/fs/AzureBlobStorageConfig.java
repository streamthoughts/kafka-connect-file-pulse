/*
 * Copyright 2019-2021 StreamThoughts.
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
import java.util.Optional;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.types.Password;

public class AzureBlobStorageConfig extends AbstractConfig {

    private static final String GROUP_AZURE = "Azure Blob Storage";

    public static final String AZURE_BLOB_STORAGE_CONNECTION_STRING_CONFIG = "azure.storage.connection.string";
    private static final String AZURE_BLOB_STORAGE_CONNECTION_STRING_DOC = "Azure storage account connection string";

    public static final String AZURE_BLOB_STORAGE_ACCOUNT_NAME_CONFIG = "azure.storage.account.name";
    public static final String AZURE_BLOB_STORAGE_ACCOUNT_NAME_DOC    = "The Azure storage account name.";

    public static final String AZURE_BLOB_STORAGE_ACCOUNT_KEY_CONFIG = "azure.storage.account.key";
    public static final String AZURE_BLOB_STORAGE_ACCOUNT_KEY_DOC = "The Azure storage account key.";

    public static final String AZURE_BLOB_STORAGE_CONTAINER_NAME_CONFIG = "azure.storage.container.name";
    private static final String AZURE_BLOB_STORAGE_CONTAINER_NAME_DOC = "The Azure storage container name";

    public final static String AZURE_BLOB_STORAGE_PREFIX_CONFIG = "azure.storage.blob.prefix";
    private final static String AZURE_BLOB_STORAGE_PREFIX_DOC = "The prefix to be used for restricting the listing of the blobs in the container";

    public AzureBlobStorageConfig(final Map<String, ?> originals) {
        super(getConf(), originals, false);
    }

    /**
     * @return the {@link ConfigDef}.
     */
    static ConfigDef getConf() {
        int azureGroupCounter = 0;

        return new ConfigDef()
                .define(
                        AZURE_BLOB_STORAGE_CONNECTION_STRING_CONFIG,
                        ConfigDef.Type.PASSWORD,
                        null,
                        ConfigDef.Importance.HIGH,
                        AZURE_BLOB_STORAGE_CONNECTION_STRING_DOC,
                        GROUP_AZURE,
                        azureGroupCounter++,
                        ConfigDef.Width.NONE,
                        AZURE_BLOB_STORAGE_CONNECTION_STRING_CONFIG
                )
                .define(
                        AZURE_BLOB_STORAGE_ACCOUNT_NAME_CONFIG,
                        ConfigDef.Type.STRING,
                        null,
                        ConfigDef.Importance.HIGH,
                        AZURE_BLOB_STORAGE_ACCOUNT_NAME_DOC,
                        GROUP_AZURE,
                        azureGroupCounter++,
                        ConfigDef.Width.NONE,
                        AZURE_BLOB_STORAGE_ACCOUNT_NAME_CONFIG
                )
                .define(
                        AZURE_BLOB_STORAGE_ACCOUNT_KEY_CONFIG,
                        ConfigDef.Type.PASSWORD,
                        null,
                        ConfigDef.Importance.HIGH,
                        AZURE_BLOB_STORAGE_ACCOUNT_KEY_DOC,
                        GROUP_AZURE,
                        azureGroupCounter++,
                        ConfigDef.Width.NONE,
                        AZURE_BLOB_STORAGE_ACCOUNT_KEY_CONFIG
                )
                .define(
                        AZURE_BLOB_STORAGE_CONTAINER_NAME_CONFIG,
                        ConfigDef.Type.STRING,
                        ConfigDef.Importance.HIGH,
                        AZURE_BLOB_STORAGE_CONTAINER_NAME_DOC,
                        GROUP_AZURE,
                        azureGroupCounter++,
                        ConfigDef.Width.NONE,
                        AZURE_BLOB_STORAGE_CONTAINER_NAME_CONFIG
                )
                .define(
                        AZURE_BLOB_STORAGE_PREFIX_CONFIG,
                        ConfigDef.Type.STRING,
                        null,
                        ConfigDef.Importance.HIGH,
                        AZURE_BLOB_STORAGE_PREFIX_DOC,
                        GROUP_AZURE,
                        azureGroupCounter++,
                        ConfigDef.Width.NONE,
                        AZURE_BLOB_STORAGE_PREFIX_CONFIG
                );
    }

    public Optional<String> getAccountName() {
        return Optional.ofNullable(getString(AZURE_BLOB_STORAGE_ACCOUNT_NAME_CONFIG));
    }

    public Optional<Password> getAccountKey() {
        return Optional.ofNullable(getPassword(AZURE_BLOB_STORAGE_ACCOUNT_KEY_CONFIG));
    }

    public Optional<Password> getConnectionString() {
        return Optional.ofNullable(getPassword(AZURE_BLOB_STORAGE_CONNECTION_STRING_CONFIG));
    }

    public String getContainerName() {
        return getString(AZURE_BLOB_STORAGE_CONTAINER_NAME_CONFIG);
    }

    public String getPrefix() {return getString(AZURE_BLOB_STORAGE_PREFIX_CONFIG);}
}
