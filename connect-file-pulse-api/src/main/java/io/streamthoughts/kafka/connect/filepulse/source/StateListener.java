/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.source;

public interface StateListener {

    /**
     * This method is invoked when a file is scheduled by the task.
     * @see FileRecordsPollingConsumer
     *
     * @param context   the file context.
     */
    void onScheduled(final FileObjectContext context);

    /**
     * This method is invoked when a file can't be scheduled.
     * @see FileRecordsPollingConsumer
     *
     * @param context   the file context.
     */
    void onInvalid(final FileObjectContext context);

    /**
     * This method is invoked when a source file is starting to be read.
     * @see FileRecordsPollingConsumer
     *
     * @param context   the file context.
     */
    void onStart(final FileObjectContext context);

    /**
     * This method is invoked when a source file processing is completed.
     * @see FileRecordsPollingConsumer
     *
     * @param context   the file context.
     */
    void onCompleted(final FileObjectContext context);

    /**
     * This method is invoked when an error occurred while processing a source file.
     * @see FileRecordsPollingConsumer
     *
     * @param context   the file context.
     */
    void onFailure(final FileObjectContext context, final Throwable t);
}
