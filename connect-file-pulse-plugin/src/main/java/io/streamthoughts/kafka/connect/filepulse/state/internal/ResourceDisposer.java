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

/**
 * A {@code MemoryResourceDisposer} can be used to dispose a shared
 * resource after it is not used any more.
 *
 * @see io.streamthoughts.azkarra.commons.rocksdb.internal.OpaqueMemoryResource
 * @param <E>   the exception type.
 */

@FunctionalInterface
public interface ResourceDisposer<E extends Throwable> {

    /**
     * Release the memory shared resource.
     *
     * @throws E    if the resource cannot be disposed.
     */
    void dispose() throws E;
}
