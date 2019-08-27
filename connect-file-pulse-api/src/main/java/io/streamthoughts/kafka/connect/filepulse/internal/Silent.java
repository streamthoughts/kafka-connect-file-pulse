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
