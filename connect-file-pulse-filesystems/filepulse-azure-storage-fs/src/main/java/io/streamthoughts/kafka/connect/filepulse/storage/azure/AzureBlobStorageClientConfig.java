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
package io.streamthoughts.kafka.connect.filepulse.storage.azure;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import java.util.Map;


public class AzureBlobStorageClientConfig extends AbstractConfig {

    private static final String GROUP_AZURE = "AZURE";

    public static final String AZURE_BLOB_STORAGE_CONNECTION_STRING_CONFIG = "azure.blob.storage.connection.string";
    private static final String AZURE_BLOB_STORAGE_CONNECTION_STRING_DOC = "Azure storage account connection string";

    public static final String AZURE_BLOB_STORAGE_CONTAINER_NAME_CONFIG = "azure.blob.storage.container.name";
    private static final String AZURE_BLOB_STORAGE_CONTAINER_NAME_DOC = "The container name";

    public final static String AZURE_BLOB_STORAGE_PREFIX_CONFIG = "azure.blob.storage.prefix";
    private final static String AZURE_BLOB_STORAGE_PREFIX_DOC = "The prefix to be used for restricting the listing of the blobs in the container";

    public AzureBlobStorageClientConfig(final Map<String, ?> originals) {
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
                        ConfigDef.Type.STRING,
                        ConfigDef.Importance.HIGH,
                        AZURE_BLOB_STORAGE_CONNECTION_STRING_DOC,
                        GROUP_AZURE,
                        azureGroupCounter++,
                        ConfigDef.Width.NONE,
                        AZURE_BLOB_STORAGE_CONNECTION_STRING_CONFIG
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
                        ConfigDef.Importance.HIGH,
                        AZURE_BLOB_STORAGE_PREFIX_DOC,
                        GROUP_AZURE,
                        azureGroupCounter++,
                        ConfigDef.Width.NONE,
                        AZURE_BLOB_STORAGE_PREFIX_CONFIG
                );
    }

    public String getConnectionString() {
        return getString(AZURE_BLOB_STORAGE_CONNECTION_STRING_CONFIG);
    }

    public String getContainerName() {
        return getString(AZURE_BLOB_STORAGE_CONTAINER_NAME_CONFIG);
    }

    public String getAzureBlobStoragePrefix() {return getString(AZURE_BLOB_STORAGE_PREFIX_CONFIG);}
}
