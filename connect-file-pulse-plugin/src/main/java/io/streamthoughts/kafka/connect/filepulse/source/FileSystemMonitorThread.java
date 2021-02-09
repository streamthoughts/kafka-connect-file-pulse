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

import io.streamthoughts.kafka.connect.filepulse.fs.FileSystemScanner;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Default thread scheduling periodic scans of the targeted file-system.
 */
public class FileSystemMonitorThread extends Thread {

    private static final Logger LOG = LoggerFactory.getLogger(FileSystemMonitorThread.class);

    private static final long SHUTDOWN_TIMEOUT_MS = 5000L;

    private final ConnectorContext context;
    private final CountDownLatch shutdownLatch;
    private final CountDownLatch waitingLatch;
    private final long scanIntervalMs;

    private final FileSystemScanner scanner;

    /**
     * Creates a new {@link FileSystemMonitorThread} instance.
     *
     * @param context the connector context.
     * @param scanner the file system scanner.
     */
    FileSystemMonitorThread(final ConnectorContext context,
                            final FileSystemScanner scanner,
                            final long scanIntervalMs) {
        super(FileSystemMonitorThread.class.getSimpleName());
        Objects.requireNonNull(context,"context can't be null");
        Objects.requireNonNull(scanner,"scanner can't be null");
        if (scanIntervalMs < 0) {
            throw new IllegalArgumentException("Invalid Argument - scanInternalMs cannot be inferior to 0");
        }
        this.context = context;
        this.scanner = scanner;
        this.scanIntervalMs = scanIntervalMs;
        this.shutdownLatch = new CountDownLatch(1);
        this.waitingLatch = new CountDownLatch(1);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void run() {
        try {
            LOG.info("Starting thread monitoring filesystem.");
            while (shutdownLatch.getCount() > 0) {
                long started = Time.SYSTEM.milliseconds();
                try {
                    scanner.scan(context);
                } catch (Exception e) {
                    LOG.error("Unexpected error while scanning file system.", e);
                    context.raiseError(e);
                    throw e;
                }

                long timeout = Math.abs(scanIntervalMs - (Time.SYSTEM.milliseconds() - started));
                LOG.info("Waiting {} ms to scan for new files.", timeout);
                boolean shuttingDown = shutdownLatch.await(timeout, TimeUnit.MILLISECONDS);
                if (shuttingDown) {
                    return;
                }
            }
        } catch (InterruptedException e) {
            LOG.error("Unexpected InterruptedException, ignoring: ", e);
        } finally {
            LOG.info("Stopped thread monitoring filesystem.");
            waitingLatch.countDown();
        }
    }

    void shutdown() {
        shutdown(SHUTDOWN_TIMEOUT_MS);
    }

    void shutdown(final long timeoutMs) {
        LOG.info("Shutting down thread monitoring filesystem.");
        this.shutdownLatch.countDown();
        try {
            this.waitingLatch.await(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ignore) {
            LOG.error("Timeout : scan loop is not terminated yet.");
        }
    }
}