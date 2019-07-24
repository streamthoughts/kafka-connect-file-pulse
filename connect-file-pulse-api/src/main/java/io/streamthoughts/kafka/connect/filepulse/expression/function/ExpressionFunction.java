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

import io.streamthoughts.kafka.connect.filepulse.data.TypedValue;
import org.apache.kafka.connect.data.SchemaAndValue;

/**
 * Default interface to define a function that can be used into an expression.
 *
 * @param <T>   the type of {@link Arguments}.
 */
public interface ExpressionFunction<T extends Arguments> {

    /**
     * Returns the case-insensitive function name.
     *
     * @return the function name.
     */
    default String name() {
        return functionNameFor(this);
    }

    /**
     * Prepares the arguments that will be passed to {@link #apply(TypedValue, Arguments)}.
     *
     * @param args  list of {@link TypedValue} arguments.
     * @return  an instance of {@link Arguments}.
     */
    T prepare(final TypedValue[] args);

    /**
     * Checks whether this function accepts the specified value.
     *
     * @param value the value to be checked.
     * @return  {@code true} if this function can be executed on the value.
     */
    default boolean accept(final TypedValue value) {
        return true;
    }

    /**
     * Executes the function on the specified value for the specified arguments.
     *
     * @param field the field on which to apply the function.
     * @param args  the function arguments.
     *
     * @return  a new {@link SchemaAndValue}.
     */
    TypedValue apply(final TypedValue field, final T args);

    static String functionNameFor(final ExpressionFunction function) {
        // simple class name conversion to camelCase
        StringBuilder b = new StringBuilder();
        final String className = function.getClass().getSimpleName();
        boolean firstChar = true;
        for (char c : className.toCharArray()) {
            if (c >= 'A' && c<= 'Z') {
                if (!firstChar) {
                    b.append("_");
                }
                b.append(c);
            } else if ( (c >= 'a' && c<= 'z') || (c>='0' && c<= '9') ) {
                b.append(c);
            } else if ( c>= ' '){
                b.append("_");
            }
            firstChar = false;
        }
        return b.toString().toLowerCase();
    }
}
