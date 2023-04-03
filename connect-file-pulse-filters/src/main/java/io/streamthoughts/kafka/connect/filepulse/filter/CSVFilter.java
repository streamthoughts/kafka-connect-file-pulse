/*
 * Copyright 2022 StreamThoughts.
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

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.ICSVParser;
import java.io.IOException;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

public class CSVFilter extends AbstractDelimitedRowFilter<CSVFilter> {

    public final static String PARSER_SEPARATOR_CONFIG = "separator";
    private final static String PARSER_SEPARATOR_DOC = "Sets the delimiter to use for separating entries.";

    public final static String PARSER_IGNORE_QUOTATIONS_CONFIG = "ignore.quotations";
    private final static String PARSER_IGNORE_QUOTATIONS_DOC =
            "Sets the ignore quotations mode - if true, quotations are ignored.";

    public final static String PARSER_ESCAPE_CHAR_CONFIG = "escape.char";
    private final static String PARSER_ESCAPE_CHAR_DOC =
            "Sets the character to use for escaping a separator or quote.";

    public final static String PARSER_IGNORE_LEADING_WHITESPACE_CONFIG = "ignore.leading.whitespace";
    private final static String PARSER_IGNORE_LEADING_WHITESPACE_DOC =
            "Sets the ignore leading whitespace setting - if true, white space in "
            + "front of a quote in a field is ignored.";

    public final static String PARSER_QUOTE_CHAR_CONFIG = "quote.char";
    private final static String PARSER_QUOTE_CHAR_DOC = "Sets the character to use for quoted elements.";

    public final static String PARSER_STRICT_QUOTES_CHAR_CONFIG = "strict.quotes";
    private final static String PARSER_STRICT_QUOTES_CHAR_DOC =
            "Sets the strict quotes setting - if true, characters outside the quotes are ignored.";
    private static final String CONFIG_GROUP = "CSV Filter";

    private CSVParser parser;

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(final Map<String, ?> configs) {
        super.configure(configs);
        this.parser = new CSVParserBuilder()
                .withSeparator(filterConfig().getString(PARSER_SEPARATOR_CONFIG).charAt(0))
                .withIgnoreQuotations(filterConfig().getBoolean(PARSER_IGNORE_QUOTATIONS_CONFIG))
                .withEscapeChar(filterConfig().getString(PARSER_ESCAPE_CHAR_CONFIG).charAt(0))
                .withIgnoreLeadingWhiteSpace(filterConfig().getBoolean(PARSER_IGNORE_LEADING_WHITESPACE_CONFIG))
                .withQuoteChar(filterConfig().getString(PARSER_QUOTE_CHAR_CONFIG).charAt(0))
                .withStrictQuotes(filterConfig().getBoolean(PARSER_STRICT_QUOTES_CHAR_CONFIG))
                .build();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected String[] parseColumnsValues(final String line) {
        try {
            return parser.parseLine(line);
        } catch (IOException e) {
            throw new FilterException("Failed to parse CSV line", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConfigDef configDef() {
        int filterGroupCounter = 0;
        return super.configDef()
                .define(
                        PARSER_SEPARATOR_CONFIG,
                        ConfigDef.Type.STRING,
                        String.valueOf(ICSVParser.DEFAULT_SEPARATOR),
                        new NonEmptyCharacter(),
                        ConfigDef.Importance.HIGH,
                        PARSER_SEPARATOR_DOC,
                        CONFIG_GROUP,
                        filterGroupCounter++,
                        ConfigDef.Width.NONE,
                        PARSER_SEPARATOR_CONFIG
                )
                .define(
                        PARSER_IGNORE_QUOTATIONS_CONFIG,
                        ConfigDef.Type.BOOLEAN,
                        ICSVParser.DEFAULT_IGNORE_QUOTATIONS,
                        ConfigDef.Importance.MEDIUM,
                        PARSER_IGNORE_QUOTATIONS_DOC,
                        CONFIG_GROUP,
                        filterGroupCounter++,
                        ConfigDef.Width.NONE,
                        PARSER_IGNORE_QUOTATIONS_CONFIG
                )
                .define(
                        PARSER_ESCAPE_CHAR_CONFIG,
                        ConfigDef.Type.STRING,
                        String.valueOf(ICSVParser.DEFAULT_ESCAPE_CHARACTER),
                        new NonEmptyCharacter(),
                        ConfigDef.Importance.MEDIUM,
                        PARSER_ESCAPE_CHAR_DOC,
                        CONFIG_GROUP,
                        filterGroupCounter++,
                        ConfigDef.Width.NONE,
                        PARSER_ESCAPE_CHAR_CONFIG
                )
                .define(
                        PARSER_IGNORE_LEADING_WHITESPACE_CONFIG,
                        ConfigDef.Type.BOOLEAN,
                        ICSVParser.DEFAULT_IGNORE_LEADING_WHITESPACE,
                        ConfigDef.Importance.MEDIUM,
                        PARSER_IGNORE_LEADING_WHITESPACE_DOC,
                        CONFIG_GROUP,
                        filterGroupCounter++,
                        ConfigDef.Width.NONE,
                        PARSER_IGNORE_LEADING_WHITESPACE_CONFIG
                )
                .define(
                        PARSER_QUOTE_CHAR_CONFIG,
                        ConfigDef.Type.STRING,
                        String.valueOf(ICSVParser.DEFAULT_QUOTE_CHARACTER),
                        new NonEmptyCharacter(),
                        ConfigDef.Importance.MEDIUM,
                        PARSER_QUOTE_CHAR_DOC,
                        CONFIG_GROUP,
                        filterGroupCounter++,
                        ConfigDef.Width.NONE,
                        PARSER_QUOTE_CHAR_CONFIG
                )
                .define(
                        PARSER_STRICT_QUOTES_CHAR_CONFIG,
                        ConfigDef.Type.BOOLEAN,
                        ICSVParser.DEFAULT_STRICT_QUOTES,
                        ConfigDef.Importance.MEDIUM,
                        PARSER_STRICT_QUOTES_CHAR_DOC,
                        CONFIG_GROUP,
                        filterGroupCounter++,
                        ConfigDef.Width.NONE,
                        PARSER_STRICT_QUOTES_CHAR_CONFIG
                );
    }


    public static class NonEmptyCharacter implements ConfigDef.Validator {
        public NonEmptyCharacter() {}

        @Override
        public void ensureValid(final String name, final Object o) {
            String s = (String)o;
            if (s != null) {
                if (s.isEmpty()) {
                    throw new ConfigException(name, o, "Character must be non-empty");
                }
                if (s.length() > 1) {
                    throw new ConfigException(
                        name,
                        o,
                        "Expected value to be a single character, but it was a string with a length superior to 1"
                    );
                }
            }

        }

        public String toString() {
            return "non-empty character";
        }
    }
}
