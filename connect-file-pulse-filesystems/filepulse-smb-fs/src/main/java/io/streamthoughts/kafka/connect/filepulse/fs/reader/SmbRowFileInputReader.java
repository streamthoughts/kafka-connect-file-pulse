/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.fs.reader;

import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.fs.SmbFileStorage;
import io.streamthoughts.kafka.connect.filepulse.fs.SmbFileSystemListingConfig;
import io.streamthoughts.kafka.connect.filepulse.fs.reader.text.RowFileInputIteratorConfig;
import io.streamthoughts.kafka.connect.filepulse.fs.reader.text.RowFileInputIteratorFactory;
import io.streamthoughts.kafka.connect.filepulse.reader.FileInputIterator;
import io.streamthoughts.kafka.connect.filepulse.reader.StorageAwareFileInputReader;
import io.streamthoughts.kafka.connect.filepulse.source.FileRecord;
import java.net.URI;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SMB file input reader for row-based file formats.
 */
public class SmbRowFileInputReader extends AbstractFileInputReader
        implements StorageAwareFileInputReader<SmbFileStorage> {

    private static final Logger LOG = LoggerFactory.getLogger(SmbRowFileInputReader.class);

    private SmbFileStorage storage;
    private RowFileInputIteratorFactory factory;

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(Map<String, ?> configs) {
        super.configure(configs);

        LOG.debug("Configuring SmbRowFileInputReader");

        if (storage == null) {
            storage = initStorage(configs);
            LOG.debug("SMB storage instantiated successfully");
        }

        this.factory = new RowFileInputIteratorFactory(
            new RowFileInputIteratorConfig(configs),
            storage,
            iteratorManager());
    }

    SmbFileStorage initStorage(Map<String, ?> configs) {
        final SmbFileSystemListingConfig config = new SmbFileSystemListingConfig(configs);
        return new SmbFileStorage(config);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SmbFileStorage storage() {
        return storage;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected FileInputIterator<FileRecord<TypedStruct>> newIterator(URI objectURI, IteratorManager iteratorManager) {
        LOG.info("Getting new iterator for SMB object '{}'", objectURI);
        return factory.newIterator(objectURI);
    }
}
