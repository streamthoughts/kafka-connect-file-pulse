/*
 * Copyright 2019-2020 StreamThoughts.
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

import io.streamthoughts.kafka.connect.filepulse.expression.EvaluationContext;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.StreamSupport;

public class Arguments<T extends Argument> implements Iterable<T> {

    @SafeVarargs
    public static <T extends Argument> Arguments<T> of(final T... arguments) {
        return new Arguments<>(Arrays.asList(arguments));
    }

    public static <T extends Argument> Arguments<T> empty() {
        return new Arguments<T>() {
            @Override
            public Iterator<T> iterator() {
                return Collections.emptyIterator();
            }

            @Override
            public String toString() {
                return "[]";
            }
        };
    }

    private final List<T> arguments;

    /**
     * Creates a new {@link Arguments} instance.
     */
    public Arguments() {
        this(new LinkedList<>());
    }

    /**
     * Creates a new {@link Arguments} instance.
     *
     * @param argument the single argument.
     */
    public Arguments(final T argument) {
        this(Collections.singletonList(argument));
    }

    /**
     * Creates a new {@link Arguments} instance.
     * @param arguments the list of arguments.
     *
     */
    public Arguments(final List<T> arguments) {
        this.arguments = arguments;
    }

    private Arguments<T> add(final T argument) {
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
    public T get(final int index) {
        return arguments.get(index);
    }

    public List<T> get(final int index, final int to) {
        return arguments.subList(index, to);
    }

    public int size() {
        return arguments.size();
    }

    @SuppressWarnings("unchecked")
    public <V> V valueOf(final String name) {
        Objects.requireNonNull(name, "name cannot be null");
        Optional<Object> value = arguments
            .stream()
            .filter(a -> a.name().equals(name))
            .findFirst()
            .map(Argument::value);
        if (value.isPresent()) return (V) value.get();

        throw new IllegalArgumentException("No argument with name '" + name + "'");
    }

    Arguments<GenericArgument> evaluate(final EvaluationContext context) {
        Arguments<GenericArgument> evaluated = new Arguments<>();
        for (T arg : arguments) {
            Object value = arg.evaluate(context);
            evaluated.add(new GenericArgument<>(arg.name(), value));
        }
        return evaluated;
    }

    public boolean valid() {
        return StreamSupport
            .stream(this.spliterator(), true)
            .allMatch(Argument::isValid);
    }

    String buildErrorMessage() {
        final StringBuilder errors = new StringBuilder();
        for (T value : arguments) {
            if (!value.errorMessages().isEmpty()) {
                List<String> errorMessages = value.errorMessages();
                for (String error : errorMessages) {
                    errors
                        .append("\n\t")
                        .append("Invalid argument with name='")
                        .append(value.name()).append("'")
                        .append(", value=")
                        .append("'").append(value.value()).append("'")
                        .append(" - ")
                        .append(error)
                        .append("\n\t");
                }
            }
        }
        return errors.toString();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<T> iterator() {
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
