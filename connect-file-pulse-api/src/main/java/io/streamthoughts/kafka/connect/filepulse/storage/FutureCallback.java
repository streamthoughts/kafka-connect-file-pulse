/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.storage;


public class FutureCallback<T> extends ConvertingFutureCallback<T, T> {

    FutureCallback(Callback<T> underlying) {
        super(underlying);
    }

    public FutureCallback() {
        super(null);
    }

    @Override
    public T convert(T result) {
        return result;
    }
}
