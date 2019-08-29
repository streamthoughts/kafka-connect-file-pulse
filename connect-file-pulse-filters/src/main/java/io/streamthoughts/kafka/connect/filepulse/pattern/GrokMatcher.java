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
package io.streamthoughts.kafka.connect.filepulse.pattern;

import org.jcodings.specific.UTF8Encoding;
import org.joni.Option;
import org.joni.Regex;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class GrokMatcher {

    private final Map<String, GrokPattern> patternsByName;

    private final List<GrokPattern> patterns;

    private final String pattern;

    private Regex regex;

    /**
     * Creates a new {@link GrokMatcher} instance.
     *
     * @param patterns  the list of patterns.
     * @param matcher   the withMessage matcher.
     */
    GrokMatcher(final List<GrokPattern> patterns,
                final String matcher) {
        Objects.requireNonNull(patterns, "pattern can't be null");
        Objects.requireNonNull(matcher, "pattern can't be null");
        this.patterns = patterns;
        this.pattern = matcher;
        this.patternsByName = patterns
                .stream()
                .collect(Collectors.toMap(GrokPattern::syntax, p -> p,  (p1, p2) -> p1.semantic() != null ? p1 : p2));
    }

    public GrokPattern getGrokPattern(final int i) {
        return patterns.get(i);
    }

    public GrokPattern getGrokPattern(final String name) {
        return patternsByName.get(name);
    }

    public Regex regex() {
        if (regex == null) {
            byte[] bytes = pattern.getBytes(StandardCharsets.UTF_8);
            regex = new Regex(bytes, 0, bytes.length, Option.NONE, UTF8Encoding.INSTANCE);
        }
        return regex;
    }

    @Override
    public String toString() {
        return "GrokMatcher{" +
                "patterns=" + patterns +
                ", pattern='" + pattern + '\'' +
                '}';
    }
}
