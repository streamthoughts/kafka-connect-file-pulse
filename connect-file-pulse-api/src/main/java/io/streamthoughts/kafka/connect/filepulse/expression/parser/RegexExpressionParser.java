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
package io.streamthoughts.kafka.connect.filepulse.expression.parser;

import io.streamthoughts.kafka.connect.filepulse.data.TypeValue;
import io.streamthoughts.kafka.connect.filepulse.expression.Expression;
import io.streamthoughts.kafka.connect.filepulse.expression.ExpressionException;
import io.streamthoughts.kafka.connect.filepulse.expression.SimpleExpression;
import io.streamthoughts.kafka.connect.filepulse.expression.SubstitutionExpression;
import io.streamthoughts.kafka.connect.filepulse.expression.ValueExpression;
import io.streamthoughts.kafka.connect.filepulse.expression.function.ExpressionFunctionExecutor;
import io.streamthoughts.kafka.connect.filepulse.expression.function.ExpressionFunctionExecutorFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Simple {@link ExpressionParser} implementation that use regex to parse expression.
 */
public class RegexExpressionParser implements ExpressionParser {

    private static final String GROUP_ROOT_OBJECT = "rootObject";
    private static final String GROUP_ATTRIBUTE   = "attribute";
    private static final String GROUP_METHOD      = "method";
    private static final String GROUP_ARGS        = "args";

    private static final String SELECTOR_EXPRESSION_REGEX =
            "(?<rootObject>[$\\w]+)(?:\\.(?<attribute>[\\.\\w]+))?";

    private static Pattern SUBSTITUTION_EXPRESSION_PATTERN =
            Pattern.compile("(?<expression>\\{\\{\\s*.+?\\s*\\}\\})+?");

    private Collection<SimpleExpressionMatcher> matchers = Arrays.asList(
            new SelectorExpressionMatcher(),
            new FunctionExpressionMatcher());

    /**
     * {@inheritDoc}
     */
    @Override
    public Expression parseExpression(final String expression) {

        Matcher matcher = SUBSTITUTION_EXPRESSION_PATTERN.matcher(expression);

        SubstitutionExpression substitutionExpression = null;

        while (matcher.find()) {

            final String originalSubstitutionExpression = matcher.group();
            final SimpleExpression replacement =  parseSubstitutionExpression(originalSubstitutionExpression);

            if (substitutionExpression == null) {
                substitutionExpression = new SubstitutionExpression(
                    expression,
                    matcher.start(),
                    matcher.end(),
                    replacement);
            } else {
                substitutionExpression.addReplacement(
                    matcher.start(),
                    matcher.end(),
                    replacement);
            }
        }

        if (substitutionExpression == null) {
            return new ValueExpression(expression);
        }

        return substitutionExpression;
    }

    SimpleExpression parseSubstitutionExpression(final String expression) {
        SimpleExpression matched = null;
        Iterator<SimpleExpressionMatcher> iterator = matchers.iterator();

        while (iterator.hasNext() && matched == null) {
            matched = iterator.next().matches(expression);
        }

        if (matched != null) {
            return matched;
        }
        throw new ExpressionException("Invalid substitution expression '" + expression + "'");
    }

    private static String trimSubstitutionExpression(final String replacementExpression) {
        return replacementExpression.substring(2, replacementExpression.length() - 2).trim();
    }

    private interface SimpleExpressionMatcher {

        SimpleExpression matches(final String expression);
    }

    private static class SelectorExpressionMatcher implements SimpleExpressionMatcher {

        private static Pattern PATTERN = Pattern.compile("^(?:" + SELECTOR_EXPRESSION_REGEX + ")$");

        /**
         * {@inheritDoc}
         */
        @Override
        public SimpleExpression matches(final String expression) {

            final String trimmed = trimSubstitutionExpression(expression);
            Matcher matcher = PATTERN.matcher(trimmed);
            if (matcher.matches()) {
                final String rootObject = matcher.group(GROUP_ROOT_OBJECT);
                final String attribute  = matcher.group(GROUP_ATTRIBUTE);
                return new SimpleExpression(expression, rootObject, attribute);
            }
            return null;
        }
    }

    private static class FunctionExpressionMatcher implements SimpleExpressionMatcher {

        private static Pattern PATTERN =
            Pattern.compile("^(?:(?<method>[\\w_]+)\\((?:" + SELECTOR_EXPRESSION_REGEX + ")(?:,(?<args>.+))*?\\))$");

        /**
         * {@inheritDoc}
         */
        @Override
        public SimpleExpression matches(final String expression) {

            final String trimmed = trimSubstitutionExpression(expression);
            Matcher matcher = PATTERN.matcher(trimmed);
            if (matcher.matches()) {
                final String rootObject = matcher.group(GROUP_ROOT_OBJECT);
                final String attribute  = matcher.group(GROUP_ATTRIBUTE);
                final String method     = matcher.group(GROUP_METHOD);
                final String args       = matcher.group(GROUP_ARGS);

                TypeValue[] parsedArgs = new TypeValue[]{};
                if (args != null) {
                    parsedArgs = Arrays
                        .stream(args.split(","))
                        .map(s -> TypeValue.of(s.trim())).toArray(TypeValue[]::new);
                }

                ExpressionFunctionExecutor function = ExpressionFunctionExecutorFactory
                    .getInstance()
                    .make(method, parsedArgs);
                return new SimpleExpression(expression, rootObject, attribute, function);
            }
            return null;
        }
    }

}
