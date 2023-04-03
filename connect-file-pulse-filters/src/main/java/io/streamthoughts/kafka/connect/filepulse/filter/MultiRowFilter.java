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

import io.streamthoughts.kafka.connect.filepulse.config.MultiRowFilterConfig;
import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.reader.RecordsIterable;
import io.streamthoughts.kafka.connect.filepulse.source.FileRecord;
import io.streamthoughts.kafka.connect.filepulse.source.FileRecordOffset;
import io.streamthoughts.kafka.connect.filepulse.source.TypedFileRecord;
import io.streamthoughts.kafka.connect.transform.pattern.GrokMatcher;
import io.streamthoughts.kafka.connect.transform.pattern.GrokPatternCompiler;
import io.streamthoughts.kafka.connect.transform.pattern.GrokPatternResolver;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.joni.Matcher;
import org.joni.Option;
import org.joni.Regex;

public class MultiRowFilter extends AbstractRecordFilter<MultiRowFilter> {

    private static final String DEFAULT_SOURCE_FIELD = "message";

    private String separator;

    private boolean negate;

    private GrokMatcher matcher;

    private final Collection<String> latest = new LinkedList<>();

    private FileRecordOffset offset;

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(final Map<String, ?> configs) {
        super.configure(configs);
        MultiRowFilterConfig configs1 = new MultiRowFilterConfig(configs);

        GrokPatternCompiler compiler = new GrokPatternCompiler(
                new GrokPatternResolver(
                        configs1.patternDefinitions(),
                        configs1.patternsDir()),
                true);
        matcher = compiler.compile(configs1.pattern());
        separator = configs1.separator();
        negate = configs1.negate();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConfigDef configDef() {
        return MultiRowFilterConfig.configDef();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RecordsIterable<TypedStruct> apply(final FilterContext context,
                                              final TypedStruct record,
                                              final boolean hasNext) throws FilterException {

        final List<TypedStruct> next = new LinkedList<>();

        final String message = record.getString(DEFAULT_SOURCE_FIELD);
        if (mayNotMatchPreviousLines(message)) {
            final TypedStruct struct = buildOutputStruct();
            next.add(struct);
            latest.clear();
        }
        latest.add(message);

        if (!hasNext) {
            final TypedStruct struct = buildOutputStruct();
            next.add(struct);
        }
        offset = context.offset();
        return new RecordsIterable<>(next);
    }

    private TypedStruct buildOutputStruct() {
        return TypedStruct.create().put(DEFAULT_SOURCE_FIELD, mergeMultiLines(latest));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void clear() {
        latest.clear();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RecordsIterable<FileRecord<TypedStruct>> flush() {
        if (latest.isEmpty()) RecordsIterable.empty();
        TypedStruct data = buildOutputStruct();
        return RecordsIterable.of(new TypedFileRecord(offset, data));
    }

    private boolean mayNotMatchPreviousLines(final String message) {
        boolean contains = isInputContainsPattern(message);
        return ((!negate && !contains) || (negate && contains)) && !latest.isEmpty();
    }

    /**
     * Checks whether the configured pattern can be found into the specified defaultStruct.
     *
     * @param message   the input defaultStruct
     * @return          {@code true} if a matches is found.
     */
    private boolean isInputContainsPattern(final String message) {
        final Regex regex = matcher.regex();
        byte[] bytes = message.getBytes(StandardCharsets.UTF_8);
        Matcher matcher = regex.matcher(bytes);
        return -1 != matcher.search(0, bytes.length, Option.DEFAULT);
    }

    /**
     * Merge all specified lines into a single one.
     *
     * @param multiLines  the row to be merged.
     * @return            the single merged row.
     */
    private String mergeMultiLines(final Collection<String> multiLines) {
        return String.join(separator, multiLines);
    }
}
