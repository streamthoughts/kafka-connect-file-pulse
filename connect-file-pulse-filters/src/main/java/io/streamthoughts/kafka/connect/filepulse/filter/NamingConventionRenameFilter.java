/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.filter;

import static io.streamthoughts.kafka.connect.filepulse.config.NamingConvention.namingConventionNotFoundErrorMsg;

import io.streamthoughts.kafka.connect.filepulse.config.NamingConvention;
import io.streamthoughts.kafka.connect.filepulse.config.NamingConventionRenameFilterConfig;
import io.streamthoughts.kafka.connect.filepulse.data.TypedField;
import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.reader.RecordsIterable;
import java.util.Iterator;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.StreamSupport;
import org.apache.commons.text.CaseUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class NamingConventionRenameFilter extends AbstractRecordFilter<NamingConventionRenameFilter> {

    private static final Logger LOG = LoggerFactory.getLogger(NamingConventionRenameFilter.class);
    private NamingConventionRenameFilterConfig config;

    @Override
    public void configure(final Map<String, ?> props) {
        super.configure(props);
        config = new NamingConventionRenameFilterConfig(props);
    }

    @Override
    public ConfigDef configDef() {
        return NamingConventionRenameFilterConfig.getConfigDef();
    }

    @Override
    public RecordsIterable<TypedStruct> apply(final FilterContext context,
                                              final TypedStruct record,
                                              final boolean hasNext) throws FilterException {
        NamingConvention namingConvention = NamingConvention.getByConfigValue(config.getDefaultNamingConvention());
        Iterator<TypedField> typedFieldsIterator = record.schema().iterator();

        StreamSupport.stream(
                        Spliterators.spliteratorUnknownSize(typedFieldsIterator, Spliterator.ORDERED), false)
                .map(TypedField::name)
                .peek(name -> LOG.trace("Renaming field {} to {}", name, renameField(name, namingConvention)))
                .forEach(name -> record.rename(name, renameField(name, namingConvention)));

        return new RecordsIterable<>(record);
    }

    String renameField(String columnName, NamingConvention namingConvention) {
        switch (namingConvention) {
            case CAMEL_CASE:
                return toCamelCase(columnName);
            case PASCAL_CASE:
                return toPascalCase(columnName);
            case SNAKE_CASE:
                return toSnakeCase(columnName);
            default:
                throw new ConfigException(namingConventionNotFoundErrorMsg(namingConvention.getConfigValue()));
        }
    }

    private String toCamelCase(String columnName) {
        return CaseUtils.toCamelCase(columnName, false, config.getColumnHeaderDelimiters());
    }

    private String toPascalCase(String columnName) {
        return CaseUtils.toCamelCase(columnName, true, config.getColumnHeaderDelimiters());
    }

    private String toSnakeCase(String columnName) {
        return camelCaseToSnakeCase(toCamelCase(columnName));
    }

    private String camelCaseToSnakeCase(String str) {
        String regex = "([a-z0-9])([A-Z]+)";
        String replacement = "$1_$2";

        return str.replaceAll(regex, replacement).toLowerCase();
    }
}