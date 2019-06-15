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
package io.streamthoughts.kafka.connect.filepulse.expression.function;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class ArgumentValue {

    private String name;
    private Object value;
    private final List<String> errorMessages;

    /**
     * Creates a new {@link ArgumentValue} instance.
     *
     * @param name  the argument name.
     * @param value the argument value.
     */
    public ArgumentValue(final String name,
                         final Object value) {
        this(name, value, Collections.emptyList());
    }

    /**
     * Creates a new {@link ArgumentValue} instance.
     *
     * @param name  the argument name.
     * @param value the argument value.
     * @param errorMessage the argument exception if is invalid.
     */
    public ArgumentValue(final String name,
                         final Object value,
                         final String errorMessage) {
        this(name, value, Collections.singletonList(errorMessage));
    }

    /**
     * Creates a new {@link ArgumentValue} instance.
     *
     * @param name  the argument name.
     * @param value the argument value.
     * @param errorMessages the argument exception if is invalid.
     */
    public ArgumentValue(final String name,
                         final Object value,
                         final List<String> errorMessages) {
        Objects.requireNonNull(name, "name can't be null");
        this.name = name;
        this.value = value;
        this.errorMessages = errorMessages;
    }

    public String name() {
        return name;
    }

    public Object value() {
        return value;
    }

    public void addErrorMessage(final String errorMessage) {
        this.errorMessages.add(errorMessage);
    }

    public List<String> errorMessages() {
        return errorMessages;
    }

    public boolean isValid() {
        return errorMessages.isEmpty();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ArgumentValue)) return false;
        ArgumentValue that = (ArgumentValue) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(value, that.value) &&
                Objects.equals(errorMessages, that.errorMessages);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, value, errorMessages);
    }

    @Override
    public String toString() {
        return "{" +
                "name='" + name + '\'' +
                ", value=" + value +
                ", errorMessages=" + errorMessages +
                '}';
    }
}
