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
package io.streamthoughts.kafka.connect.filepulse.filter;

import io.streamthoughts.kafka.connect.filepulse.config.MultiRowFilterConfig;
import io.streamthoughts.kafka.connect.filepulse.pattern.GrokMatcher;
import io.streamthoughts.kafka.connect.filepulse.pattern.GrokPatternCompiler;
import io.streamthoughts.kafka.connect.filepulse.pattern.GrokPatternResolver;
import io.streamthoughts.kafka.connect.filepulse.reader.FileInputRecord;
import io.streamthoughts.kafka.connect.filepulse.reader.RecordsIterable;
import io.streamthoughts.kafka.connect.filepulse.source.FileInputData;
import io.streamthoughts.kafka.connect.filepulse.source.FileInputOffset;
import org.apache.kafka.common.config.ConfigDef;
import org.joni.Matcher;
import org.joni.Option;
import org.joni.Regex;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class MultiRowFilter extends AbstractRecordFilter<MultiRowFilter> {

    private MultiRowFilterConfig configs;

    private String separator;

    private boolean negate;

    private GrokPatternCompiler compiler;

    private GrokMatcher matcher;

    private final Collection<String> latest = new LinkedList<>();

    private FileInputOffset offset;

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(final Map<String, ?> configs) {
        super.configure(configs);
        this.configs = new MultiRowFilterConfig(configs);

        compiler = new GrokPatternCompiler(
            new GrokPatternResolver(
                this.configs.patternDefinitions(),
                this.configs.patternsDir()),
            true);
        matcher = compiler.compile(this.configs.pattern());
        separator = this.configs.separator();
        negate = this.configs.negate();
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
    public RecordsIterable<FileInputData> apply(final FilterContext context,
                                                final FileInputData record,
                                                final boolean hasNext) {

        final List<FileInputData> next = new LinkedList<>();
        final String message = record.getFirstValueForField(FileInputData.DEFAULT_MESSAGE_FIELD);
        if (mayNotMatchPreviousLines(message)) {
            next.add(FileInputData.defaultStruct(mergeMultiLines(latest)));
            latest.clear();
        }
        latest.add(message);

        if (!hasNext) {
            next.add(FileInputData.defaultStruct(mergeMultiLines(latest)));
        }
        offset = context.offset();
        return new RecordsIterable<>(next);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RecordsIterable<FileInputRecord> flush() {
        if (latest.isEmpty()) RecordsIterable.empty();

        FileInputData data = FileInputData.defaultStruct(mergeMultiLines(latest));
        return new RecordsIterable<>(new FileInputRecord(offset, data));
    }

    private boolean mayNotMatchPreviousLines(final String message) {
        boolean contains = isInputContainsPattern(message);
        return ((!negate && !contains) || (negate && contains)) && !latest.isEmpty();
    }

    /**
     * Checks whether the configured pattern can be found into the specified defaultStruct.
     *
     * @param message   the input defaultStruct
     * @return          <code>true</code> if a matches is found.
     */
    private boolean isInputContainsPattern(final String message) {
        final Regex regex = matcher.regex();
        byte[] bytes = message.getBytes();
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
