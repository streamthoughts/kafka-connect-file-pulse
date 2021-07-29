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
package io.streamthoughts.kafka.connect.filepulse.source;

import io.streamthoughts.kafka.connect.filepulse.storage.StateBackingStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * Default class to report file state progression into Kafka.
 */
public class KafkaFileStateReporter implements StateListener {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaFileStateReporter.class);

    private final StateBackingStore<FileObject> store;

    /**
     * Creates a new {@link KafkaFileStateReporter} instance.
     *
     * @param store         the store to be used.
     */
    KafkaFileStateReporter(final StateBackingStore<FileObject> store) {
        Objects.requireNonNull(store, "store can't be null");
        this.store = store;
    }

    /**
     * Notify a state change for the specified source file.
     *
     * @param key       the object file key.
     * @param metadata  the object file metadata.
     * @param offset    the object file offset.
     * @param status    the status.
     */
    void notify(final FileObjectKey key,
                final FileObjectMeta metadata,
                final FileObjectOffset offset,
                final FileObjectStatus status
                ) {
        Objects.requireNonNull(metadata, "metadata can't be null");
        Objects.requireNonNull(offset, "offset can't be null");
        Objects.requireNonNull(status, "status can't be null");
        store.putAsync(key.original(), new FileObject(metadata, offset, status));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onScheduled(final FileContext context) {
        Objects.requireNonNull(context, "context can't be null");
        LOG.debug("Scheduling source file '{}'", context.metadata());
        notify(context.key(), context.metadata(), context.offset(), FileObjectStatus.SCHEDULED);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onInvalid(final FileContext context) {
        Objects.requireNonNull(context, "context can't be null");
        notify(context.key(), context.metadata(), context.offset(), FileObjectStatus.INVALID);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onStart(final FileContext context) {
        Objects.requireNonNull(context, "context can't be null");
        LOG.debug("Starting to precess source file '{}'", context.metadata());
        notify(context.key(), context.metadata(), context.offset(), FileObjectStatus.STARTED);
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void onCompleted(final FileContext context) {
        Objects.requireNonNull(context, "context can't be null");
        LOG.debug("Completed source file '{}'", context.metadata());
        notify(context.key(), context.metadata(), context.offset(), FileObjectStatus.COMPLETED);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onFailure(final FileContext context, final Throwable t) {
        Objects.requireNonNull(context, "context can't be null");
        LOG.error("Error while processing source file '{}'", context.metadata(), t);
        notify(context.key(), context.metadata(), context.offset(), FileObjectStatus.FAILED);
    }

}