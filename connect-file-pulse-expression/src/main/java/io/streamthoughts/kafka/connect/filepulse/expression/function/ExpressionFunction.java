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

import io.streamthoughts.kafka.connect.filepulse.data.TypedValue;
import io.streamthoughts.kafka.connect.filepulse.expression.Expression;
import org.apache.kafka.connect.data.SchemaAndValue;

import java.util.ArrayList;
import java.util.List;

/**
 * Default interface to define a function that can be used into an expression.
 */
public interface ExpressionFunction {

    /**
     * Returns the case-insensitive function name.
     *
     * @return the function name.
     */
    default String name() {
        return functionNameFor(this);
    }

    /**
     * Prepares the arguments that will be passed to {@link #validate(Arguments)}.
     *
     * @param args  list of {@link TypedValue} arguments.
     * @return  an instance of {@link Arguments}.
     */
    default Arguments<?> prepare(final Expression[] args) {
        if (args.length == 0) return Arguments.empty();
        List<Argument> arguments = new ArrayList<>();
        for (int i = 0; i < args.length; i++) {
            arguments.add(new ExpressionArgument(String.valueOf(i), args[i]));
        }
        return new Arguments<>(arguments);
    }

    /**
     * Checks whether this function accepts the given arguments.
     *
     * @param   arguments the arguments value to be checked.
     * @return  {@code true} if this function can be executed with the given arguments.
     */
    default Arguments<GenericArgument> validate(final Arguments<GenericArgument> arguments) {
        return arguments;
    }

    /**
     * Executes the function on the specified value for the specified arguments.
     *
     * @param args  the function arguments.
     *
     * @return  a new {@link SchemaAndValue}.
     */
    TypedValue apply(final Arguments<GenericArgument> args);

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
