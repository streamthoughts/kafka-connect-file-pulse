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
package io.streamthoughts.kafka.connect.filepulse.expression.parser.antlr4;

import io.streamthoughts.kafka.connect.filepulse.data.TypedValue;
import io.streamthoughts.kafka.connect.filepulse.expression.Expression;
import io.streamthoughts.kafka.connect.filepulse.expression.ExpressionException;
import io.streamthoughts.kafka.connect.filepulse.expression.FunctionExpression;
import io.streamthoughts.kafka.connect.filepulse.expression.PropertyExpression;
import io.streamthoughts.kafka.connect.filepulse.expression.SubstitutionExpression;
import io.streamthoughts.kafka.connect.filepulse.expression.SubstitutionExpression.ReplacementExpression;
import io.streamthoughts.kafka.connect.filepulse.expression.ValueExpression;
import io.streamthoughts.kafka.connect.filepulse.expression.function.ExpressionFunctionExecutor;
import io.streamthoughts.kafka.connect.filepulse.expression.function.ExpressionFunctionExecutors;
import io.streamthoughts.kafka.connect.filepulse.expression.parser.ExpressionParser;
import io.streamthoughts.kafka.connect.filepulse.expression.parser.ScELParseCancellationException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.BufferedTokenStream;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

/**
 * {@link ExpressionParser} based on Antlr4 Lexer/Parser
 */
public class Antlr4ExpressionParser implements ExpressionParser {

    private static final String DEFAULT_ROOT_OBJECT = "value";

    /**
     * {@inheritDoc}
     */
    @Override
    public Expression parseExpression(final String expression) throws ExpressionException {
        return parseExpression(expression, DEFAULT_ROOT_OBJECT);
    }

    public Expression parseExpression(final String expression, final String defaultScope) throws ExpressionException {

        final CharStream cs = CharStreams.fromString(expression);

        ScELLexer lexer = new ScELLexer(cs);
        ScELParser parser = new ScELParser(new BufferedTokenStream(lexer));
        parser.removeErrorListeners();
        parser.addErrorListener(new ThrowingErrorListener(expression));

        ParseTreeWalker walker = new ParseTreeWalker();
        ExpressionProvider factory = new ExpressionProvider(expression, defaultScope);

        walker.walk(factory, parser.scel());

        return factory.expression();
    }

    public static class ThrowingErrorListener extends BaseErrorListener {

        private final String expression;

        ThrowingErrorListener(final String expression) {
            this.expression = expression;
        }

        /**
         * {@inheritDoc}
         * @throws ScELParseCancellationException
         */
        @Override
        public void syntaxError(final Recognizer<?, ?> recognizer,
                                final Object offendingSymbol,
                                final int line,
                                final int charPositionInLine,
                                final String msg,
                                final RecognitionException e)
                throws ScELParseCancellationException {
            throw new ScELParseCancellationException(
                "Cannot parse ScEL expression '" + expression
                + "' : line " + line + ":" + charPositionInLine + " " + msg);
        }
    }

    private static class ExpressionProvider extends ScELParserBaseListener {

        public static final String NULL_STRING = "null";
        private final String originalExpression;
        private final String defaultScope;

        ExpressionProvider(final String originalExpression, final String defaultScope) {
            this.originalExpression = originalExpression;
            this.defaultScope = defaultScope;
        }

        private final IdentityHashMap<ParserRuleContext, ContextExpressions> contexts = new IdentityHashMap<>();

        private ContextExpressions current = new ContextExpressions(null);

        public Expression expression() {
            if (current.expressions.isEmpty()) {
                throw new ExpressionException(
                    "Invalid expression after parsing:" +  originalExpression + ". Empty result");
            }
            if (current.expressions.size() > 1) {
                throw new ExpressionException(
                    "Invalid expression after parsing:" +  originalExpression + ". Too many results");
            }
            return current.expressions.remove();
        }

        @Override
        public void exitSubstitutionExpression(final ScELParser.SubstitutionExpressionContext ctx) {

            final SubstitutionExpression substitution = new SubstitutionExpression(originalExpression);
            while (!current.expressions.isEmpty()) {
                ReplacementExpression expression = (ReplacementExpression) current.expressions.poll();
                substitution.addReplacement(expression);
            }
            current.expressions.add(substitution);
        }

        @Override
        public void enterSubstitutionStrExpression(final ScELParser.SubstitutionStrExpressionContext ctx) {
            current = new ContextExpressions(current);
            contexts.put(ctx, current);
        }

        @Override
        public void exitSubstitutionStrExpression(final ScELParser.SubstitutionStrExpressionContext ctx) {
            final ContextExpressions contextExpression = contexts.remove(ctx);

            final int start = ctx.LineSubstExprStart().getSymbol().getStartIndex();
            final int stop = ctx.LineSubstExprEnd().getSymbol().getStopIndex() + 1;
            List<Expression> expressions = new ArrayList<>();
            while (!current.expressions.isEmpty()) {
                expressions.add(current.expressions.remove());
            }

            ReplacementExpression expression = new ReplacementExpression(ctx.getText(), start, stop, expressions);
            if (contextExpression.parent != null) {
                contextExpression.parent.expressions.add(expression);
                current = contextExpression.parent;
            } else {
                current.expressions.add(expression);
            }
        }

        @Override
        public void enterFunctionDeclaration(final ScELParser.FunctionDeclarationContext ctx) {
            current = new ContextExpressions(current);
            contexts.put(ctx, current);
        }

        @Override
        public void exitFunctionDeclaration(final ScELParser.FunctionDeclarationContext ctx) {
            final ContextExpressions contextExpression = contexts.remove(ctx);

            final ArrayDeque<Expression> argsExpressions = contextExpression.expressions;

            List<Expression> arguments = new ArrayList<>(argsExpressions.size());
            while (!argsExpressions.isEmpty()) {
                arguments.add(argsExpressions.remove());
            }

            ExpressionFunctionExecutor executor = ExpressionFunctionExecutors.resolve(
                ctx.Identifier().getText(),
                arguments.toArray(new Expression[0])
            );

            FunctionExpression expression = new FunctionExpression(ctx.getText(), executor);
            if (contextExpression.parent != null) {
                contextExpression.parent.expressions.add(expression);
                current = contextExpression.parent;
            } else {
                current.expressions.add(expression);
            }
        }

        @Override
        public void exitPropertyDeclaration(final ScELParser.PropertyDeclarationContext ctx) {
            final PropertyExpression expression = new PropertyExpression(
                    ctx.getText(),
                    ctx.scope() != null ? ctx.scope().getText() : defaultScope,
                    ctx.attribute() != null ? ctx.attribute().getText() : null
            );
            current.expressions.add(expression);
        }

        @Override
        public void exitValue(final ScELParser.ValueContext ctx) {
            final String originalExpression = ctx.getText();

            Object value;
            if (originalExpression.equalsIgnoreCase(NULL_STRING)) {
                value = null;
            } else if (originalExpression.startsWith("'") && originalExpression.endsWith("'")) {
                final String s = originalExpression.substring(1);
                value = s.substring(0, s.length() - 1);
            } else {
                value = TypedValue.parse(originalExpression).value();
            }
            current.expressions.add(new ValueExpression(originalExpression, value));
        }
    }

    private static class ContextExpressions {

        final ContextExpressions parent;
        final ArrayDeque<Expression> expressions = new ArrayDeque<>();

        ContextExpressions(final ContextExpressions parent) {
            this.parent = parent;
        }
    }
}
