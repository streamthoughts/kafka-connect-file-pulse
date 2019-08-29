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

import io.streamthoughts.kafka.connect.filepulse.data.Type;

public class GrokPattern {

    private final String syntax;

    private final String semantic;

    private final Type type;

    /**
     * Creates a new {@link GrokPattern} instance.
     * @param syntax    the grok pattern syntax.
     * @param semantic  the grok pattern semantic.
     * @param type      the grok pattern type.
     */
    GrokPattern(final String syntax,
                final String semantic,
                final String type) {
        this(syntax, semantic, toType(type));
    }

    /**
     * Creates a new {@link GrokPattern} instance.
     *
     * @param syntax    the grok pattern syntax.
     * @param semantic  the grok pattern semantic.
     * @param type      the grok pattern type.
     */
    GrokPattern(final String syntax,
                final String semantic,
                final Type type) {
        this.syntax = syntax;
        this.semantic = semantic;
        this.type = type;
    }

    public String syntax() {
        return syntax;
    }

    public String semantic() {
        return semantic;
    }

    public Type type() {
        return type;
    }

    private static Type toType(final String type) {
       return (type != null) ? Type.valueOf(type.toUpperCase()) : Type.STRING;
    }

    @Override
    public String toString() {
        return "GrokPattern{" +
                "syntax='" + syntax + '\'' +
                ", semantic='" + semantic + '\'' +
                ", type='" + type + '\'' +
                '}';
    }
}
