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

import io.streamthoughts.kafka.connect.filepulse.config.GrokFilterConfig;
import io.streamthoughts.kafka.connect.filepulse.data.Type;
import io.streamthoughts.kafka.connect.filepulse.internal.SchemaUtils;
import io.streamthoughts.kafka.connect.filepulse.pattern.GrokMatcher;
import io.streamthoughts.kafka.connect.filepulse.pattern.GrokPattern;
import io.streamthoughts.kafka.connect.filepulse.pattern.GrokPatternCompiler;
import io.streamthoughts.kafka.connect.filepulse.pattern.GrokPatternResolver;
import io.streamthoughts.kafka.connect.filepulse.pattern.GrokSchemaBuilder;
import io.streamthoughts.kafka.connect.filepulse.reader.RecordsIterable;
import io.streamthoughts.kafka.connect.filepulse.source.FileInputData;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.joni.Matcher;
import org.joni.NameEntry;
import org.joni.Option;
import org.joni.Regex;
import org.joni.Region;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class GrokFilter extends AbstractMergeRecordFilter<GrokFilter> {

    private GrokFilterConfig configs;

    private GrokPatternCompiler compiler;

    private List<GrokMatcher> patterns;

    private Schema schema;

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(final Map<String, ?> props) {
        super.configure(props);
        configs = new GrokFilterConfig(props);
        compiler = new GrokPatternCompiler(
                new GrokPatternResolver(
                        configs.patternDefinitions(),
                        configs.patternsDir()),
                        configs.namedCapturesOnly());
        patterns = Collections.singletonList(compiler.compile(configs.pattern()));
        schema = GrokSchemaBuilder.buildSchemaForGrok(patterns);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConfigDef configDef() {
        return GrokFilterConfig.configDef();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected RecordsIterable<FileInputData> apply(final FilterContext context, final FileInputData record) {

        final String value = record.value().getString(configs.source());

        if (value == null) return null;

        final Struct struct = new Struct(schema);

        final byte[] bytes = value.getBytes();
        for (GrokMatcher grok : patterns) {
            final Regex regex = grok.regex();
            final Matcher matcher = regex.matcher(bytes);
            int result = matcher.search(0, bytes.length, Option.DEFAULT);
            if (result != -1) {
                extractAndPutFieldsTo(struct, regex, matcher, bytes, grok);
                return new RecordsIterable<>(new FileInputData(struct));
            }
        }
        throw new FilterException("Can not matches grok pattern on value : " + value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Set<String> overwrite() {
        return configs.overwrite();
    }

    private void extractAndPutFieldsTo(final Struct struct,
                                       final Regex regex,
                                       final Matcher matcher,
                                       final byte[] bytes,
                                       final GrokMatcher grok) {
        final Region region = matcher.getEagerRegion();
        for (Iterator<NameEntry> entry = regex.namedBackrefIterator(); entry.hasNext(); ) {
            NameEntry e = entry.next();
            final String field = GrokSchemaBuilder.getStringFieldName(e);
            final GrokPattern pattern = grok.getGrokPattern(field);
            final Type type = pattern != null ? pattern.type() : Type.STRING;
            List<Object> objects = extractValuesForEntry(region, e, bytes, type);
            if (SchemaUtils.isTypeOf(schema.field(field), Schema.Type.ARRAY)) {
                struct.put(field, objects);
            } else if (objects.size() > 0){
                struct.put(field, objects.get(0));
            }
        }
    }

    private List<Object> extractValuesForEntry(final Region region,
                                               final NameEntry e,
                                               final byte[] bytes,
                                               final Type target) {
        final List<Object> values = new ArrayList<>(e.getBackRefs().length);
        for (int i = 0; i < e.getBackRefs().length; i++) {
            int capture = e.getBackRefs()[i];
            int begin = region.beg[capture];
            int end = region.end[capture];

            if (begin > -1 && end > -1) {
                Object value = new String(bytes, begin, end - begin, StandardCharsets.UTF_8);
                if (target != null) {
                    value = target.convert(value);
                }
                values.add(value);
            }
        }
        return values;
    }
}