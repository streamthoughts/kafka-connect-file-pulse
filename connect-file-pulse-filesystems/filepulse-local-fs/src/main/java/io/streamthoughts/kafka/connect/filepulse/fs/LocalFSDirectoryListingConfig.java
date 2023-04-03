/*
 * Copyright 2019-2020 StreamThoughts.
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

public class LocalFSDirectoryListingConfig extends AbstractConfig {


    public static final String FS_LISTING_DIRECTORY_PATH = "fs.listing.directory.path";
    public static final String FS_LISTING_DIRECTORY_DOC = "The input directory to scan";

    public static final String FS_RECURSIVE_SCAN_ENABLE_CONFIG  = "fs.listing.recursive.enabled";
    private static final String FS_RECURSIVE_SCAN_ENABLE_DOC    = "Boolean indicating whether local directory " +
                                                                  "should be recursively scanned (default true).";

    public static ConfigDef getConf() {
        return new ConfigDef()
            .define(
                    FS_LISTING_DIRECTORY_PATH,
                    ConfigDef.Type.STRING,
                    ConfigDef.Importance.HIGH,
                    FS_LISTING_DIRECTORY_DOC
            )
                
            .define(
                    FS_RECURSIVE_SCAN_ENABLE_CONFIG,
                    ConfigDef.Type.BOOLEAN,
                    true,
                    ConfigDef.Importance.MEDIUM,
                    FS_RECURSIVE_SCAN_ENABLE_DOC
            );
    }

    /**
     * Creates a new {@link LocalFSDirectoryListingConfig} instance.
     * @param originals the configuration.
     */
    public LocalFSDirectoryListingConfig(final Map<?, ?> originals) {
        super(getConf(), originals, false);
    }

    public boolean isRecursiveScanEnable() {
        return getBoolean(FS_RECURSIVE_SCAN_ENABLE_CONFIG);
    }

    public String listingDirectoryPath() {
        return this.getString(FS_LISTING_DIRECTORY_PATH);
    }
}
