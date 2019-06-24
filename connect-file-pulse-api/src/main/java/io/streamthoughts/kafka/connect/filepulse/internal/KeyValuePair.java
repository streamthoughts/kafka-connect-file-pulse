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
package io.streamthoughts.kafka.connect.filepulse.internal;

import java.util.Objects;

public class KeyValuePair<K, V> {

    public static KeyValuePair<String, String> parse(final String str, final String separator) {
        String[] split = str.split(separator);
        return of(split[0], split[1]);
    }

    public static  <K, V>  KeyValuePair<K, V> of(final K key, final V value) {
        return new KeyValuePair<>(key, value);
    }

    /**
     * Creates a new {@link KeyValuePair} instance.
     * @param key   the key.
     * @param value the value.
     */
    private KeyValuePair(final K key, final V value) {
        this.key = key;
        this.value = value;
    }

    public final K key;

    public final V value;

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (!(o instanceof KeyValuePair)) return false;
        KeyValuePair<?, ?> that = (KeyValuePair<?, ?>) o;
        return Objects.equals(key, that.key) &&
                Objects.equals(value, that.value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return Objects.hash(key, value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "[" +
                "key=" + key +
                ", value=" + value +
                ']';
    }
}
