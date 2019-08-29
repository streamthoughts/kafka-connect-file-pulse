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
package io.streamthoughts.kafka.connect.filepulse.expression.parser.regex;

import io.streamthoughts.kafka.connect.filepulse.expression.PropertyExpression;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AccessExpressionMatcher implements RegexExpressionMatcher {

    private static final String GROUP_ROOT_OBJECT = "rootObject";
    private static final String GROUP_ATTRIBUTE   = "attribute";

    private static final String SELECTOR_ROOT_OBJECT_EXPRESSION_REGEX =
            "\\$(?<rootObject>\\w+)";

    private static final String SELECTOR_ROOT_OBJECT_AND_ATTRIBUTE_EXPRESSION_REGEX =
            "(?:\\$(?<rootObject>\\w+)\\.)?(?<attribute>[\\.\\w]+)";

    private static Pattern[] PATTERNS;
    static {
        PATTERNS = new Pattern[]{
                Pattern.compile("^(?:" + SELECTOR_ROOT_OBJECT_EXPRESSION_REGEX + ")$"),
                Pattern.compile("^(?:" + SELECTOR_ROOT_OBJECT_AND_ATTRIBUTE_EXPRESSION_REGEX + ")$")
        };
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PropertyExpression matches(final String expression,
                                      final String defaultRootObject,
                                      final boolean substitution) {

        final String trimmed = substitution ? trimSubstitutionExpression(expression) : expression;

        for (Pattern p : PATTERNS) {
            Matcher matcher = p.matcher(trimmed);
            if (matcher.matches()) {

                final String rootObject = matcher.group(GROUP_ROOT_OBJECT);
                String attribute = null;
                if (matcher.groupCount() == 2) {
                    attribute = matcher.group(GROUP_ATTRIBUTE);
                }

                return new PropertyExpression(
                        expression,
                        rootObject != null ? rootObject : defaultRootObject,
                        attribute
                );
            }
        }
        return null;
    }

    private static String trimSubstitutionExpression(final String expression) {
        return expression.substring(2, expression.length() - 2).trim();
    }
}