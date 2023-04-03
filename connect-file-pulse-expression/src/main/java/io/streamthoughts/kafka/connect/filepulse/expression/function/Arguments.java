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

import io.streamthoughts.kafka.connect.filepulse.expression.Expression;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class Arguments implements Iterable<Argument> {

    public static Arguments of(final Argument... arguments) {
        return new Arguments(Arrays.asList(arguments));
    }

    public static Arguments of(final String name1, final Expression expression1) {
        return of(new ExpressionArgument(name1, expression1));
    }

    public static Arguments of(final String name1, final Expression expression1,
                               final String name2, final Expression expression2) {
        return of(
                new ExpressionArgument(name1, expression1),
                new ExpressionArgument(name2, expression2)
        );
    }

    public static Arguments of(final String name1, final Expression expression1,
                               final String name2, final Expression expression2,
                               final String name3, final Expression expression3) {
        return of(
                new ExpressionArgument(name1, expression1),
                new ExpressionArgument(name2, expression2),
                new ExpressionArgument(name3, expression3)
        );
    }

    public static Arguments empty() {
        return new Arguments() {
            @Override
            public Iterator<Argument> iterator() {
                return Collections.emptyIterator();
            }

            @Override
            public String toString() {
                return "[]";
            }
        };
    }

    private final List<Argument> arguments;

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
    public Arguments(final Argument argument) {
        this(Collections.singletonList(argument));
    }

    /**
     * Creates a new {@link Arguments} instance.
     * @param arguments the list of arguments.
     *
     */
    public Arguments(final List<Argument> arguments) {
        this.arguments = arguments;
    }

    private Arguments add(final Argument argument) {
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
    public Argument get(final int index) {
        return arguments.get(index);
    }

    public List<Argument> get(final int index, final int to) {
        return arguments.subList(index, to);
    }

    public int size() {
        return arguments.size();
    }

    @SuppressWarnings("unchecked")
    public <V> Optional<V> valueOf(final String name) {
        Objects.requireNonNull(name, "name cannot be null");
        Optional<Object> value = arguments
            .stream()
            .filter(a -> a.name().equals(name))
            .findFirst()
            .map(Argument::value);

        return (Optional<V>) value;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<Argument> iterator() {
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
