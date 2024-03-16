/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.internal;

import java.util.function.Function;

public class Silent {

    @FunctionalInterface
    public interface CheckedFunction<T> {
        T apply() throws Exception;
    }

    @FunctionalInterface
    public interface CheckedOperation {

        void apply() throws Exception;
    }

    public static void unchecked(final CheckedOperation function) {
        unchecked(function, RuntimeException::new);
    }

    public static <T> T unchecked(final CheckedFunction<T> function) {
        return unchecked(function, RuntimeException::new);
    }

    public static void unchecked(final CheckedOperation function,
                                 final Function<Exception, RuntimeException> wrapper) {
        try {
            function.apply();
        } catch (final Exception e) {
            throw wrapper.apply(e);
        }
    }

    public static <T> T unchecked(final CheckedFunction<T> function,
                                  final Function<Exception, RuntimeException> wrapper) {
        try {
            return function.apply();
        } catch (Exception e) {
            throw wrapper.apply(e);
        }
    }
}
