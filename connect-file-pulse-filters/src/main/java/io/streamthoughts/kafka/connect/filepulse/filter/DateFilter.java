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

import io.streamthoughts.kafka.connect.filepulse.config.DateFilterConfig;
import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.expression.Expression;
import io.streamthoughts.kafka.connect.filepulse.expression.StandardEvaluationContext;
import io.streamthoughts.kafka.connect.filepulse.expression.parser.ExpressionParsers;
import io.streamthoughts.kafka.connect.filepulse.internal.DateTimeParser;
import io.streamthoughts.kafka.connect.filepulse.reader.RecordsIterable;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.errors.ConnectException;

public class DateFilter extends AbstractRecordFilter<DateFilter> {

    private DateFilterConfig config;

    private Expression fieldExpression;

    private Expression targetExpression;

    private  ZoneId zoneId;

    private List<DateTimeParser> dtp;

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(final Map<String, ?> props) {
        super.configure(props);
        config = new DateFilterConfig(props);

        // Parse expression while supporting substitution
        fieldExpression = ExpressionParsers.parseExpression(config.field());
        targetExpression = ExpressionParsers.parseExpression(config.target());

        if (config.formats().isEmpty()) {
            throw new ConnectException("Invalid configuration, at least one date format must be provided");
        }


        zoneId = config.timezone();
        dtp = new ArrayList<>(config.formats().size());
        final Locale locale = config.locale();
        for (String format : config.formats()) {
            try {
                dtp.add(new DateTimeParser(format, locale));
            } catch (IllegalArgumentException e) {
                throw new ConnectException("Invalid configuration, cannot parse date format : " + format);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConfigDef configDef() {
        return DateFilterConfig.configDef();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RecordsIterable<TypedStruct> apply(final FilterContext context,
                                              final TypedStruct record,
                                              final boolean hasNext) throws FilterException {
        InternalFilterContext internalContext = (InternalFilterContext) context;
        internalContext.setValue(record);

        StandardEvaluationContext evaluationContext = new StandardEvaluationContext(
                internalContext,
                internalContext.variables()
        );

        final Expression field = mayEvaluateFieldExpression(evaluationContext);
        final String datetime = field.readValue(evaluationContext, String.class);

        if (datetime == null) {
            throw new FilterException("Invalid field name '" + config.field() + "'");
        }

        Exception lastException = null;

        final Expression target = mayEvaluateTargetExpression(evaluationContext);
        for (DateTimeParser parser : dtp) {
            long epochMilli = -1;
            try {
                final ZonedDateTime zdt = parser.parse(datetime, zoneId);
                epochMilli = zdt.toInstant().toEpochMilli();
            } catch (Exception e) {
                lastException = e;
                continue;
            }
            target.writeValue(epochMilli, evaluationContext);
            return RecordsIterable.of(record);
        }

        throw new FilterException(
            String.format("Failed to parse date from field '%s' with value '%s'", config.field() , datetime),
            lastException
        );
    }

    private Expression mayEvaluateTargetExpression(final StandardEvaluationContext evaluationContext) {
        if (!targetExpression.canWrite()) {
            final String evaluated = targetExpression.readValue(evaluationContext, String.class);
            return ExpressionParsers.parseExpression(evaluated);
        }
        return targetExpression;
    }

    private Expression mayEvaluateFieldExpression(final StandardEvaluationContext evaluationContext) {
        if (!fieldExpression.canWrite()) {
            final String evaluated = fieldExpression.readValue(evaluationContext, String.class);
            return ExpressionParsers.parseExpression(evaluated);
        }
        return fieldExpression;
    }
}
