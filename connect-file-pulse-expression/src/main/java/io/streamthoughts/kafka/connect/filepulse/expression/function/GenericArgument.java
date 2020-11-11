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

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

public class GenericArgument<T> implements Argument {

    private final String name;
    private final T value;
    private final List<String> errorMessages;

    /**
     * Creates a new {@link GenericArgument} instance.
     *
     * @param name  the argument name.
     * @param value the argument value.
     */
    public GenericArgument(final String name,
                           final T value) {
        this(name, value, new LinkedList<>());
    }

    /**
     * Creates a new {@link GenericArgument} instance.
     *
     * @param name  the argument name.
     * @param value the argument value.
     * @param errorMessage the argument error if is invalid.
     */
    public GenericArgument(final String name,
                            final T value,
                            final String errorMessage) {
        this(name, value, Collections.singletonList(errorMessage));
    }

    /**
     * Creates a new {@link GenericArgument} instance.
     *
     * @param name  the argument name.
     * @param value the argument value.
     * @param errorMessages the argument error if is invalid.
     */
    public GenericArgument(final String name,
                            final T value,
                            final List<String> errorMessages) {
        Objects.requireNonNull(name, "name can't be null");
        this.name = name;
        this.value = value;
        this.errorMessages = errorMessages;
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
    public T value() {
        return value;
    }

    public void addErrorMessage(final String errorMessage) {
        this.errorMessages.add(errorMessage);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<String> errorMessages() {
        return errorMessages;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isValid() {
        return errorMessages.isEmpty();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object evaluate(EvaluationContext context) {
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
                Objects.equals(value, that.value) &&
                Objects.equals(errorMessages, that.errorMessages);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return Objects.hash(name, value, errorMessages);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "{" +
                "name='" + name + '\'' +
                ", value=" + value +
                ", errorMessages=" + errorMessages +
                '}';
    }
}
