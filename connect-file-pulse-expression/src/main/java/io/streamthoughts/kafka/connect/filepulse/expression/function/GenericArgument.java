/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.expression.function;

import java.util.Objects;

public class GenericArgument implements Argument {

    private final String name;
    private final Object value;

    /**
     * Creates a new {@link GenericArgument} instance.
     *
     * @param name          the argument name.
     * @param value         the argument value.
     */
    public GenericArgument(final String name,
                           final Object value) {
        Objects.requireNonNull(name, "name can't be null");
        this.name = name;
        this.value = value;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String name() {
        return name;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object value() {
        return value;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof GenericArgument)) return false;
        GenericArgument that = (GenericArgument) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(value, that.value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return Objects.hash(name, value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "{" +
                "name='" + name + '\'' +
                ", value=" + value +
                '}';
    }
}
