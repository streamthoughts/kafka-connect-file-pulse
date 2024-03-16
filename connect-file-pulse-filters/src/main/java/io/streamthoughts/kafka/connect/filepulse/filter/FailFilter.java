/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.filter;

import io.streamthoughts.kafka.connect.filepulse.config.FailFilterConfig;
import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.expression.Expression;
import io.streamthoughts.kafka.connect.filepulse.expression.StandardEvaluationContext;
import io.streamthoughts.kafka.connect.filepulse.expression.parser.ExpressionParsers;
import io.streamthoughts.kafka.connect.filepulse.reader.RecordsIterable;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;

public class FailFilter extends AbstractRecordFilter<FailFilter> {

    private FailFilterConfig config;

    private Expression expression;

    /**
     * {@inheritDoc}
     */
    @Override
    public ConfigDef configDef() {
        return FailFilterConfig.configDef();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(final Map<String, ?> props) {
        super.configure(props);
        config = new FailFilterConfig(props);
        expression = ExpressionParsers.parseExpression(config.message());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RecordsIterable<TypedStruct> apply(final FilterContext context,
                                              final TypedStruct record,
                                              final boolean hasNext) throws FilterException {

        if (condition.apply(context, record)) {

            InternalFilterContext internalContext = (InternalFilterContext) context;
            internalContext.setValue(record);

            final StandardEvaluationContext ec = new StandardEvaluationContext(
                    internalContext,
                    context.variables());

            final String message = expression.readValue(ec, String.class);
            throw new FilterException(message);
        }
        return RecordsIterable.of(record);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean accept(final FilterContext context, final TypedStruct record) {
        // We should always accept record to filter into the apply method.
        return true;
    }
}
