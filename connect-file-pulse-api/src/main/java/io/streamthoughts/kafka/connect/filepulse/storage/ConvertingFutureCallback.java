/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.storage;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public abstract class ConvertingFutureCallback<U, T> implements Callback<U>, Future<T> {

    private final Callback<T> underlying;
    private final CountDownLatch finishedLatch;
    private T result = null;
    private Throwable exception = null;

    ConvertingFutureCallback(Callback<T> underlying) {
        this.underlying = underlying;
        this.finishedLatch = new CountDownLatch(1);
    }

    protected abstract T convert(U result);

    @Override
    public void onCompletion(Throwable error, U result) {
        this.exception = error;
        this.result = convert(result);
        if (underlying != null) {
            underlying.onCompletion(error, this.result);
        }
        finishedLatch.countDown();
    }

    @Override
    public boolean cancel(boolean b) {
        return false;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public boolean isDone() {
        return finishedLatch.getCount() == 0;
    }

    @Override
    public T get() throws InterruptedException, ExecutionException {
        finishedLatch.await();
        return result();
    }

    @Override
    public T get(long l, TimeUnit timeUnit)
            throws InterruptedException, ExecutionException, TimeoutException {
        if (!finishedLatch.await(l, timeUnit)) {
            throw new TimeoutException("Timed out waiting for future");
        }
        return result();
    }

    private T result() throws ExecutionException {
        if (exception != null) {
            throw new ExecutionException(exception);
        }
        return result;
    }
}