/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.fs.reader;

import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.data.TypedValue;
import io.streamthoughts.kafka.connect.filepulse.fs.Storage;
import io.streamthoughts.kafka.connect.filepulse.fs.reader.text.BytesRecordOffset;
import io.streamthoughts.kafka.connect.filepulse.reader.FileInputIterator;
import io.streamthoughts.kafka.connect.filepulse.reader.FileInputIteratorFactory;
import io.streamthoughts.kafka.connect.filepulse.reader.ReaderException;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectContext;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectMeta;
import io.streamthoughts.kafka.connect.filepulse.source.FileRecord;
import io.streamthoughts.kafka.connect.filepulse.source.TypedFileRecord;
import java.io.InputStream;
import java.net.URI;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

public final class LocalPropertiesFileInputReader extends BaseLocalFileInputReader {

    private PropertiesIteratorFactory factory;

    /** {@inheritDoc} **/
    @Override
    public void configure(Map<String, ?> configs) {
        super.configure(configs);
        this.factory = new PropertiesIteratorFactory(storage());
    }

    /** {@inheritDoc} **/
    @Override
    protected FileInputIterator<FileRecord<TypedStruct>> newIterator(final URI objectURI,
                                                                     final IteratorManager iteratorManager) {
        return factory.newIterator(objectURI);
    }

    public static class PropertiesIteratorFactory implements FileInputIteratorFactory {

        public static final String RECORD_NAME = "io.streamthoughts.kafka.connect.filepulse.Record";

        private final Storage storage;

        /**
         * Creates a new {@link PropertiesIteratorFactory} instance.
         *
         * @param storage the {@link Storage} to use for getting metadata.
         */
        public PropertiesIteratorFactory(final Storage storage) {
            this.storage = Objects.requireNonNull(storage, "storage should not be null");
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public FileInputIterator<FileRecord<TypedStruct>> newIterator(final URI objectURI) {
            Properties props = new Properties();
            try(InputStream is = storage.getInputStream(objectURI)) {
                props.load(is);
                TypedStruct struct = TypedStruct.create(RECORD_NAME);
                for (Map.Entry<Object, Object> entry : props.entrySet()) {
                    String path = entry.getKey().toString();
                    struct = struct.insert(path, TypedValue.any(entry.getValue()));
                }

                final FileObjectMeta metadata = storage.getObjectMetadata(objectURI);
                TypedFileRecord record = new TypedFileRecord(BytesRecordOffset.empty(), struct);
                return new DelegatingFileInputIterator(
                        new FileObjectContext(metadata),
                        Collections.singleton(record).iterator()
                );
            } catch (Exception e) {
                throw new ReaderException("Failed to create FileInputIterator for: " + objectURI, e);
            }
        }
    }

}
