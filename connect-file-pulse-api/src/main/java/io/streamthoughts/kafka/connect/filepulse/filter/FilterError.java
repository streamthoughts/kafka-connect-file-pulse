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
package io.streamthoughts.kafka.connect.filepulse.filter;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Objects;

public class FilterError {

    private final String exceptionMessage;

    private final String exceptionClassName;

    private final String exceptionStacktrace;

    private final String filter;

    /**
     * Helper method to create a new {@link FilterError} object from a given {@link Throwable}.
     */
    public static FilterError of(final Throwable throwable, final String filter) {
        final Throwable cause;
        if (throwable instanceof FilterException && throwable.getCause() != null) {
            cause = throwable.getCause();
        } else {
            cause = throwable;
        }
        return new FilterError(cause.getMessage(), cause.getClass().getName(), getStacktrace(cause), filter);
    }

    /**
     * Creates a new {@link FilterError} instance.
     *
     * @param message       the exception message.
     * @param classname     the exception className.
     * @param stacktrace    the exception stacktrace.
     * @param filter        the failed filter name.
     */
    public FilterError(final String message,
                       final String classname,
                       final String stacktrace,
                       final String filter) {
        this.exceptionMessage = Objects.requireNonNull(message, "'message' should not be null");
        this.exceptionClassName = Objects.requireNonNull(classname, "'classname' should not be null");
        this.exceptionStacktrace = Objects.requireNonNull(stacktrace, "'stacktrace' should not be null");
        this.filter = Objects.requireNonNull(filter, "'filter' should not be null");
    }

    public String filter() {
        return filter;
    }

    public String exceptionMessage() {
        return exceptionMessage;
    }

    public String exceptionStacktrace() {
        return exceptionStacktrace;
    }

    public String getExceptionClassName() {
        return exceptionClassName;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FilterError that = (FilterError) o;
        return Objects.equals(exceptionMessage, that.exceptionMessage) &&
               Objects.equals(exceptionClassName, that.exceptionClassName) &&
               Objects.equals(exceptionStacktrace, that.exceptionStacktrace) &&
               Objects.equals(filter, that.filter);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return Objects.hash(exceptionMessage, exceptionClassName, exceptionStacktrace, filter);
    }

    /**
     * @return the stacktrace representation for the given {@link Throwable}.
     */
    private static String getStacktrace(final Throwable throwable) {
        final StringWriter sw = new StringWriter();
        final PrintWriter pw = new PrintWriter(sw, true);
        throwable.printStackTrace(pw);
        return sw.getBuffer().toString();
    }
}
