/*
 * Copyright 2019-2023 StreamThoughts.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.streamthoughts.kafka.connect.filepulse.fs.reader;

import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.fs.SftpFileStorage;
import io.streamthoughts.kafka.connect.filepulse.fs.SftpFilesystemListingConfig;
import io.streamthoughts.kafka.connect.filepulse.fs.iterator.SftpRowFileInputIteratorFactory;
import io.streamthoughts.kafka.connect.filepulse.fs.reader.text.RowFileInputIteratorConfig;
import io.streamthoughts.kafka.connect.filepulse.reader.FileInputIterator;
import io.streamthoughts.kafka.connect.filepulse.reader.StorageAwareFileInputReader;
import io.streamthoughts.kafka.connect.filepulse.source.FileRecord;
import java.net.URI;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SftpRowFileInputReader extends AbstractFileInputReader
        implements StorageAwareFileInputReader<SftpFileStorage> {
    private static final Logger log = LoggerFactory.getLogger(SftpRowFileInputReader.class);
    private SftpFileStorage storage;

    private SftpRowFileInputIteratorFactory factory;

    @Override
    public void configure(Map<String, ?> configs) {
        super.configure(configs);

        log.debug("Configuring SftpRowFileInputReader");

        if (storage == null) {
            storage = initStorage(configs);
            log.debug("Storage instantiated successfully");
        }

        this.factory = initIteratorFactory(configs);
    }

    SftpRowFileInputIteratorFactory initIteratorFactory(Map<String, ?> configs) {
        return new SftpRowFileInputIteratorFactory(
                new RowFileInputIteratorConfig(configs),
                storage,
                iteratorManager()
        );
    }

    SftpFileStorage initStorage(Map<String, ?> configs) {
        final SftpFilesystemListingConfig config = new SftpFilesystemListingConfig(configs);
        return new SftpFileStorage(config);
    }

    @Override
    public SftpFileStorage storage() {
        return storage;
    }

    @Override
    protected FileInputIterator<FileRecord<TypedStruct>> newIterator(URI objectURI, IteratorManager iteratorManager) {
        log.info("Getting new iterator for object '{}'", objectURI);
        return factory.newIterator(objectURI);
    }
}
