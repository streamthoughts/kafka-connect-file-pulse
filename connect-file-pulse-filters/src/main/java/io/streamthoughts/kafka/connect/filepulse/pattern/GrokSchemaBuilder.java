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

import io.streamthoughts.kafka.connect.filepulse.internal.SchemaUtils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.joni.NameEntry;
import org.joni.Regex;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;

/**
 * Helper class to build {@link SchemaBuilder} from {@link GrokMatcher} instance.
 */
public class GrokSchemaBuilder {

    /**
     * Builds a new connect schema from the specified grok patterns.
     *
     * @param patterns  the list of grok patterns.
     * @return  a new SchemaBuilder instance.
     */
    public static Schema buildSchemaForGrok(final List<GrokMatcher> patterns) {
        SchemaBuilder schema = SchemaBuilder.struct();
        patterns.forEach(grok -> buildSchemaForGrok(schema, grok));
        return schema.build();
    }

    /**
     * Extract the field name from the specified name entry.
     *
     * @param e  the name entry to be used.
     * @return   the withMessage name.
     */
    public static String getStringFieldName(final NameEntry e) {
        return new String(e.name, e.nameP, e.nameEnd - e.nameP, StandardCharsets.UTF_8);
    }

    private static void buildSchemaForGrok(final SchemaBuilder sb, final GrokMatcher grok) {
        List<String> alreadyExistingFields = SchemaUtils.getAllFieldNames(sb);
        final Regex regex = grok.regex();

        for (Iterator<NameEntry> entry = regex.namedBackrefIterator(); entry.hasNext(); ) {
            NameEntry e = entry.next();
            final String field = getStringFieldName(e);
            if (!alreadyExistingFields.contains(field)) {
                alreadyExistingFields.add(field);
                final GrokPattern pattern = grok.getGrokPattern(field);
                Schema.Type type = pattern != null ? pattern.type().schemaType() : Schema.Type.STRING ;
                sb.field(field, defaultSchemaForType(type));
            }
        }
    }

    private static Schema defaultSchemaForType(final Schema.Type type) {
        return new SchemaBuilder(type).optional().defaultValue(null).build();
    }
}
