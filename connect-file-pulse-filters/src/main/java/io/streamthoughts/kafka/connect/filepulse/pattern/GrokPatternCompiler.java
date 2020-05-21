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
package io.streamthoughts.kafka.connect.filepulse.pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GrokPatternCompiler {

    private static final Logger LOG = LoggerFactory.getLogger(GrokPatternCompiler.class);

    private static final String SYNTAX_FIELD   = "syntax";
    private static final String SEMANTIC_FIELD = "semantic";
    private static final String TYPE_FIELD     = "type";

    private static final String REGEX = "(?:%\\{(?<syntax>[A-Z0-9_]+)(?:\\:(?<semantic>[a-zA-Z0-9_\\\\-]+))?(?:\\:(?<type>[a-zA-Z0-9_\\\\-]+))?\\})";
    private static final Pattern PATTERN = Pattern.compile(REGEX);

    private final GrokPatternResolver resolver;

    private final boolean namedCapturesOnly;

    /**
     * Creates a new {@link GrokPatternCompiler} instance.
     *
     * @param resolver              the grok pattern resolver.
     * @param namedCapturesOnly     is only named pattern should be captured.
     */
    public GrokPatternCompiler(final GrokPatternResolver resolver,
                               final boolean namedCapturesOnly) {
        Objects.requireNonNull(resolver, "resolver can't be null");
        this.resolver = resolver;
        this.namedCapturesOnly = namedCapturesOnly;
    }

    public GrokMatcher compile(final String expression) {
        Objects.requireNonNull(expression, "expression can't be null");

        LOG.info("Starting to compile grok matcher expression : {}", expression);
        ArrayList<GrokPattern> patterns = new ArrayList<>();
        final String regex = compileRegex(expression, patterns);
        LOG.info("Grok expression compiled to regex : {}", regex);
        return new GrokMatcher(patterns, regex);
    }

    private String compileRegex(final String expression, final List<GrokPattern> patterns) {
        Matcher matcher = PATTERN.matcher(expression);
        final StringBuffer sb = new StringBuffer();
        while (matcher.find()) {
            GrokPattern grok = new GrokPattern(
                    matcher.group(SYNTAX_FIELD),
                    matcher.group(SEMANTIC_FIELD),
                    matcher.group(TYPE_FIELD));

            patterns.add(grok);

            final String resolved = resolver.resolve(grok.syntax());
            String replacement = compileRegex(resolved, patterns);
            if (grok.semantic() != null) {
                replacement = capture(replacement, grok.semantic());
            } else if (!namedCapturesOnly) {
                replacement = capture(replacement, grok.syntax());
            }
            replacement = replacement
                    .replace("\\$","\\\\\\$")
                    .replace("\\", "\\\\");
            matcher.appendReplacement(sb, replacement);
        }
        // Copy the remainder of the input sequence.
        matcher.appendTail(sb);
        return sb.toString();
    }

    private String capture(final String expression, final String name) {
        return "(?<" + name + ">" + expression + ")";
    }
}
