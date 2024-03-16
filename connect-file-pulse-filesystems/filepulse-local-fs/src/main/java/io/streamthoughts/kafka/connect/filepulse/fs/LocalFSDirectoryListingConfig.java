/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
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
