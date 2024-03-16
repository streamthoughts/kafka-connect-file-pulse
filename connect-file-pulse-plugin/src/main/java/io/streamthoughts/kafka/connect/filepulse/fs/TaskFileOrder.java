/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
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
