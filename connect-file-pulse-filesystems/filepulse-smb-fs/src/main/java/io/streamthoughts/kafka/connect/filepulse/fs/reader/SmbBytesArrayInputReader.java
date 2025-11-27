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
import io.streamthoughts.kafka.connect.filepulse.fs.reader.text.BytesArrayInputIteratorFactory;
import io.streamthoughts.kafka.connect.filepulse.reader.FileInputIterator;
import io.streamthoughts.kafka.connect.filepulse.reader.StorageAwareFileInputReader;
import io.streamthoughts.kafka.connect.filepulse.source.FileRecord;
import java.net.URI;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SMB file input reader for reading entire files as byte arrays.
 * The {@code SmbBytesArrayInputReader} creates one record per input file.
 * Each record has single field {@code message} containing the content of the file as a byte array.
 */
public class SmbBytesArrayInputReader extends AbstractFileInputReader
        implements StorageAwareFileInputReader<SmbFileStorage> {

    private static final Logger LOG = LoggerFactory.getLogger(SmbBytesArrayInputReader.class);

    private SmbFileStorage storage;
    private BytesArrayInputIteratorFactory factory;

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(Map<String, ?> configs) {
        super.configure(configs);

        LOG.debug("Configuring SmbBytesArrayInputReader");

        if (storage == null) {
            storage = initStorage(configs);
            LOG.debug("SMB storage instantiated successfully");
        }

        this.factory = initIteratorFactory();
    }

    BytesArrayInputIteratorFactory initIteratorFactory() {
        return new BytesArrayInputIteratorFactory(storage, iteratorManager());
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
