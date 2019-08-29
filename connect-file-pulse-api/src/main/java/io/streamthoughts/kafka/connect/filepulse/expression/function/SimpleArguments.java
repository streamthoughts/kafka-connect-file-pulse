/*
 * Copyright 2019 StreamThoughts.
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
package io.streamthoughts.kafka.connect.filepulse.expression.function;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class SimpleArguments implements Arguments {

    private final List<ArgumentValue> arguments;

    /**
     * Creates a new {@link SimpleArguments} instance.
     */
    public SimpleArguments() {
        this(new LinkedList<>());
    }

    /**
     * Creates a new {@link SimpleArguments} instance.
     *
     * @param argument the single argument.
     */
    public SimpleArguments(final ArgumentValue argument) {
        this(Collections.singletonList(argument));
    }

    /**
     * Creates a new {@link SimpleArguments} instance.
     * @param arguments the list of arguments.
     *
     */
    public SimpleArguments(final List<ArgumentValue> arguments) {
        this.arguments = arguments;
    }

    public SimpleArguments withArg(final ArgumentValue argument) {
        arguments.add(argument);
        return this;
    }

    /**
     * Returns the argument at the specified position in this list.
     *
     * @param index index of the argument to return
     * @return the argument at the specified position in this list
     * @throws IndexOutOfBoundsException if the index is out of range
     *         ({@code index < 0 || index >= size()})
     */
    public ArgumentValue get(final int index) {
        return arguments.get(index);
    }

    @SuppressWarnings("unchecked")
    public <T> T valueOf(final String name) {
        Objects.requireNonNull(name, "name cannot be null");
        Optional<Object> value = arguments
                .stream()
                .filter(a -> a.name().equals(name))
                .findFirst()
                .map(ArgumentValue::value);
        if (value.isPresent()) return (T) value.get();

        throw new IllegalArgumentException("No argument with name '" + name + "'");

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<ArgumentValue> iterator() {
        return arguments.iterator();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return arguments.toString();
    }
}
