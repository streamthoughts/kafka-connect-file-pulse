/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.expression.function;

import io.streamthoughts.kafka.connect.filepulse.data.TypedValue;
import io.streamthoughts.kafka.connect.filepulse.expression.EvaluationContext;
import io.streamthoughts.kafka.connect.filepulse.expression.Expression;
import io.streamthoughts.kafka.connect.filepulse.expression.ExpressionException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/**
 * Default interface to define a function that can be used into an expression.
 */
public interface ExpressionFunction extends Supplier<ExpressionFunction.Instance> {

    /**
     * Returns the case-insensitive function name.
     *
     * @return the function name.
     */
    default String name() {
        return functionNameFor(this);
    }

    /**
     * @return a new {@link Instance}.
     */
    @Override
    Instance get();

    @FunctionalInterface
    interface Instance {

        /**
         * Prepares the arguments that will be evaluated and used to build
         * the {@link EvaluatedExecutionContext} pass then through the {@link #invoke}.
         *
         * @param args  list of {@link TypedValue} arguments.
         * @return  an instance of {@link Arguments}.
         */
        default Arguments prepare(final Expression[] args) throws ExpressionException {
            if (args.length == 0) return Arguments.empty();
            List<Argument> arguments = new ArrayList<>();
            for (int i = 0; i < args.length; i++) {
                arguments.add(new ExpressionArgument(String.valueOf(i), args[i]));
            }
            return new Arguments(arguments);
        }

        /**
         * Executes this function instance with the specified context and argument.
         *
         * @param context       the {@link EvaluationContext}.
         * @param arguments     the {@link Arguments}.
         * @return              the function result.
         */
        TypedValue invoke(final EvaluationContext context,
                          final Arguments arguments) throws ExpressionException;
    }

    /**
     * Helper method to compute a default function name.
     *
     * @param function  the {@link ExpressionFunction}.
     * @return          a string name.
     */
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
