/*
 * Copyright 2022 StreamThoughts.
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
package io.streamthoughts.kafka.connect.filepulse.fs;

import io.streamthoughts.kafka.connect.filepulse.source.FileObjectMeta;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Use for ordering files before assigning to task.
 */
public interface TaskFileOrder extends Function<Collection<FileObjectMeta>, List<FileObjectMeta>> {

    static TaskFileOrder findBuiltInByName(final String name) {
        return BuiltIn.valueOf(name).get();
    }

    List<FileObjectMeta> sort(final Collection<FileObjectMeta> objects);

    /**
     * {@inheritDoc}
     */
    @Override
    default List<FileObjectMeta> apply(final Collection<FileObjectMeta> objects) {
        return sort(objects);
    }

     enum BuiltIn {

         /** Sort all object files by the last-modified date. */
         LAST_MODIFIED(
                 new AbstractTaskFileOrder(Comparator.comparingLong(FileObjectMeta::lastModified))
         ),
         /** Sort all object files by URI. */
         URI(
                 new AbstractTaskFileOrder(Comparator.comparing(FileObjectMeta::uri))
         ),
         /** Sort all object files by content-length. */
         CONTENT_LENGTH(
                 new AbstractTaskFileOrder(Comparator.comparingLong(FileObjectMeta::contentLength))
         ),
         /** Sort all object files by content-length. */
         CONTENT_LENGTH_DESC(
                 new AbstractTaskFileOrder(Comparator.comparingLong(FileObjectMeta::contentLength).reversed())
         );

         final TaskFileOrder order;

         BuiltIn(TaskFileOrder order) {
             this.order = order;
         }

         TaskFileOrder get() {
             return order;
         }

    }

    class AbstractTaskFileOrder implements TaskFileOrder {

        private final Comparator<FileObjectMeta> comparator;

        protected AbstractTaskFileOrder(Comparator<FileObjectMeta> comparator) {
            this.comparator = comparator;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public List<FileObjectMeta> sort(final Collection<FileObjectMeta> objects) {
            return objects
                    .stream()
                    .sorted(comparator)
                    .collect(Collectors.toList());
        }
    }
}
