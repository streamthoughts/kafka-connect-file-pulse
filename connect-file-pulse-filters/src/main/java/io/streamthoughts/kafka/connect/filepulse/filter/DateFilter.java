/*
 * Copyright 2019 StreamThoughts.
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
import io.streamthoughts.kafka.connect.filepulse.expression.ValueExpression;
import io.streamthoughts.kafka.connect.filepulse.expression.parser.regex.RegexExpressionParser;
import io.streamthoughts.kafka.connect.filepulse.reader.RecordsIterable;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.errors.ConnectException;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class DateFilter extends AbstractRecordFilter<DateFilter> {

    private static final String DEFAULT_ROOT_OBJECT = "value";

    private DateFilterConfig config;

    private RegexExpressionParser parser;

    private Expression fieldExpression;

    private Expression targetExpression;

    private boolean mustEvaluateTargetExpression = true;

    private boolean mustEvaluateFieldExpression = true;

    private List<DateTimeFormatter> dtf;

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(final Map<String, ?> props) {
        super.configure(props);
        config = new DateFilterConfig(props);

        parser = new RegexExpressionParser();

        // Parse expression while supporting substitution
        fieldExpression = parser.parseExpression(config.field(), DEFAULT_ROOT_OBJECT);
        targetExpression = parser.parseExpression(config.target(), DEFAULT_ROOT_OBJECT);

        // Check whether the field expression can be pre-evaluated (i.e : is not a substitution expression).
        if (fieldExpression instanceof ValueExpression) {
            fieldExpression = mayEvaluateFieldExpression(new StandardEvaluationContext(new Object()));
            mustEvaluateFieldExpression = false;
        }

        // Check whether the target expression can be pre-evaluated (i.e : is not a substitution expression).
        if (targetExpression instanceof ValueExpression) {
            targetExpression = mayEvaluateTargetExpression(new StandardEvaluationContext(new Object()));
            mustEvaluateTargetExpression = false;
        }

        if (config.formats().isEmpty()) {
            throw new ConnectException("Invalid configuration, at least one date format must be provided");
        }

        final Locale locale = config.locale();
        final ZoneId timezone = config.timezone();
        dtf = new ArrayList<>(config.formats().size());
        for (String format : config.formats()) {
            try {
                final DateTimeFormatter formatter = DateTimeFormatter
                        .ofPattern(format, locale)
                        .withZone(timezone);
                dtf.add(formatter);
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
        final String date = field.readValue(evaluationContext, String.class);

        if (date == null) {
            throw new FilterException("Invalid field name '" + config.field() + "'");
        }

        Exception lastException = null;

        final Expression target = mayEvaluateTargetExpression(evaluationContext);
        for (DateTimeFormatter formatter : dtf) {
            long epochMilli = -1;
            try {
                epochMilli = Instant.from(formatter.parse(date)).toEpochMilli();
            } catch (Exception e) {
                lastException = e;
                continue;
            }
            target.writeValue(epochMilli, evaluationContext);
            return RecordsIterable.of(record);
        }

        throw new FilterException(
            String.format("Failed to parse date from field '%s' with value '%s'", config.field() , date),
            lastException
        );
    }

    private Expression mayEvaluateTargetExpression(final StandardEvaluationContext evaluationContext) {
        if (mustEvaluateTargetExpression) {
            final String evaluated = targetExpression.readValue(evaluationContext, String.class);
            return parser.parseExpression(evaluated, DEFAULT_ROOT_OBJECT, false);
        }
        return targetExpression;
    }

    private Expression mayEvaluateFieldExpression(final StandardEvaluationContext evaluationContext) {
        if (mustEvaluateFieldExpression) {
            final String evaluated = fieldExpression.readValue(evaluationContext, String.class);
            return parser.parseExpression(evaluated, DEFAULT_ROOT_OBJECT, false);
        }
        return fieldExpression;
    }
}
