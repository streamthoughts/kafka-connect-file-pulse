/*
 * Copyright 2021 StreamThoughts.
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
package io.streamthoughts.kafka.connect.filepulse.state.internal;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * An {@code OpaqueMemoryResource} represents a shared memory resource.
 *
 * @param <T>   the resource type.
 */
public class OpaqueMemoryResource<T> implements AutoCloseable {

    private final T resource;

    private final ResourceDisposer<Exception> disposer;

    private final AtomicBoolean closed = new AtomicBoolean(false);

    /**
     * Creates a new {@link OpaqueMemoryResource} instance.
     *
     * @param resource  the resource to handle.
     * @param disposer  the {@link ResourceDisposer} to be used for releasing the resource.
     */
    public OpaqueMemoryResource(final T resource,
                                final ResourceDisposer<Exception> disposer) {
        this.resource = Objects.requireNonNull(resource, "resource should not be null");;
        this.disposer = Objects.requireNonNull(disposer, "disposer should not be null");
    }

    /**
     * Gets the handle resource.
     */
    public T getResource() {
        return resource;
    }

    /**
     * Releases this resource.
     */
    @Override
    public void close() throws Exception {
        if (closed.compareAndSet(false, true)) {
            disposer.dispose();
        }
    }
}
