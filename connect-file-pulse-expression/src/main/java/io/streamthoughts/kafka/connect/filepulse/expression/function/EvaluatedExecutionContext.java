/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.expression.function;

import io.streamthoughts.kafka.connect.filepulse.data.TypedValue;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class EvaluatedExecutionContext {

    private final Map<Integer, TypedValue> argumentByIndex;
    private final Map<String, TypedValue> argumentByName;

    /**
     * Creates a new {@link EvaluatedExecutionContext} instance.
     */
    EvaluatedExecutionContext() {
        this.argumentByName = new HashMap<>();
        this.argumentByIndex = new HashMap<>();
    }

    void addArgument(final String name, final int index, final TypedValue value) {
        this.argumentByName.put(name, value);
        this.argumentByIndex.put(index, value);
    }

    /**
     * Retrieves the argument to which the specified index is mapped.
     *
     * @param index the argument index.
     * @return      the {@link TypedValue}.
     * @throws      IndexOutOfBoundsException if the given index is out of range.
     */
    public TypedValue get(final int index) {
        if (!argumentByIndex.containsKey(index)) {
            throw new IndexOutOfBoundsException(index);
        }
        return Optional.ofNullable(argumentByIndex.get(index)).orElse(TypedValue.none());
    }

    /**
     * Retrieves the argument to which the specified name is mapped.
     *
     * @param name  the argument name.
     * @return      the {@link TypedValue}.
     */
    public TypedValue get(final String name) {
        return Optional.ofNullable(argumentByName.get(name)).orElse(TypedValue.none());
    }

    public List<TypedValue> get(final int index, final int to) {
        return values().subList(index, to);
    }

    /**
     * @return the number of arguments.
     */
    public int size() {
        return argumentByName.size();
    }

    /**
     * @return values for all arguments.
     */
    public List<TypedValue> values() {
        return new ArrayList<>(argumentByIndex.values());
    }
}
