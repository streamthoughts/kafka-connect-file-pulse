/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.expression.accessor;

import io.streamthoughts.kafka.connect.filepulse.data.Type;
import io.streamthoughts.kafka.connect.filepulse.data.TypedValue;
import io.streamthoughts.kafka.connect.filepulse.expression.EvaluationContext;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Header;

public class HeadersAccessor implements PropertyAccessor {

    /**
     * {@inheritDoc}
     */
    @Override
    public Class<?>[] getSpecificTargetClasses() {
        return new Class[]{ConnectHeaders.class};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean canRead(final EvaluationContext context,
                           final Object target,
                           final String name) throws AccessException {
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TypedValue read(final EvaluationContext context,
                           final Object target,
                           final String name) throws AccessException {

        ConnectHeaders headers = (ConnectHeaders) target;

        Iterator<Header> it = headers.allWithName(name);

        List<Object> values = StreamSupport
            .stream(Spliterators.spliteratorUnknownSize(it, Spliterator.ORDERED), false)
            .map(Header::value)
            .collect(Collectors.toList());
        return TypedValue.array(values, Type.STRING);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void write(final EvaluationContext context,
                      final Object target,
                      final String name,
                      final Object newValue) throws AccessException {

        ConnectHeaders headers = (ConnectHeaders) target;
        headers.add(name, new SchemaAndValue(null, newValue));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean canWrite(final EvaluationContext context,
                            final Object target,
                            final String name) throws AccessException {
        return true;
    }
}
