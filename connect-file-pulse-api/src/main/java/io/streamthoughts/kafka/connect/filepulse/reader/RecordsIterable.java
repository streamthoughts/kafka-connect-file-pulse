/*
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
package io.streamthoughts.kafka.connect.filepulse.reader;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Default class to iterate over a bunch of records return from either a {@link FileInputReader} or
 * a {@link io.streamthoughts.kafka.connect.filepulse.filter.RecordFilter}
 *
 * @param <T> type of value.
 */
public class RecordsIterable<T> implements Iterable<T> {

    private final List<T> records;

    /**
     * Return a new {@link RecordsIterable} instance with no records.
     *
     * @param <T> type of value.
     * @return a new {@link RecordsIterable} instance
     */
    public static <T> RecordsIterable<T> empty() {
        return new RecordsIterable<>(Collections.emptyList());
    }

    @SafeVarargs
    public static <T> RecordsIterable<T> of(final T... records) {
        return new RecordsIterable<>(records);
    }

    /**
     * Creates a new {@link RecordsIterable} instance.
     * @param records   the records.
     */
    @SafeVarargs
    public RecordsIterable(final T... records) {
        this(Arrays.asList(records));
    }

    /**
     * Creates a new {@link RecordsIterable} instance.
     * @param records   the records.
     */
    public RecordsIterable(final List<T> records) {
        Objects.requireNonNull(records, "records cannot be null");
        this.records = records;
    }

    /**
     * Returns the number of records in this iterable.
     *
     * @return the number of records.
     */
    public int size() {
        return records.size();
    }

    /**
     * Checks whether this iterable contains no records.
     *
     * @return {@code true} if this collection contains no records
     */
    public boolean isEmpty() {
        return records.isEmpty();
    }

    @Override
    public Iterator<T> iterator() {
        return records.iterator();
    }

    public T last() {
        return !isEmpty() ? records.get(records.size() - 1) : null ;
    }

    @Override
    public Spliterator<T> spliterator() {
        return Spliterators.spliterator(records, 0);
    }

    public List<T> collect() {
        return records;
    }

    /**
     * Returns a sequential {@code Stream} with this collection as its source.
     *
     * @return a sequential {@code Stream} over the elements in this collection
     */
    public Stream<T> stream() {
        return StreamSupport.stream(spliterator(), false);
    }

    @Override
    public String toString() {
        return records.toString();
    }
}
