/*
 * Copyright 2023 StreamThoughts.
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

import static io.streamthoughts.kafka.connect.filepulse.config.NamingConventionFilterConfig.CSV_DEFAULT_COLUMN_RENAME_DELIMITER_CONFIG;
import static io.streamthoughts.kafka.connect.filepulse.config.NamingConventionFilterConfig.CSV_DEFAULT_COLUMN_RENAME_STRATEGY_CONFIG;
import static io.streamthoughts.kafka.connect.filepulse.config.NamingConvention.CAMEL_CASE;
import static io.streamthoughts.kafka.connect.filepulse.config.NamingConvention.PASCAL_CASE;
import static io.streamthoughts.kafka.connect.filepulse.config.NamingConvention.SNAKE_CASE;
import static java.util.Collections.emptyMap;
import static java.util.Map.of;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.mock;

import io.streamthoughts.kafka.connect.filepulse.config.NamingConvention;
import io.streamthoughts.kafka.connect.filepulse.data.TypedField;
import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.reader.RecordsIterable;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class NamingConventionFilterTest {
    @ParameterizedTest
    @MethodSource
    void when_given_column_name_should_convert_to_correct_format_based_on_strategy(String originalColumnName,
                                                                                   String renamedColumnName,
                                                                                   NamingConvention renameStrategy) {
        NamingConventionFilter namingConventionFilter = new NamingConventionFilter();
        namingConventionFilter.configure(emptyMap());
        Assertions.assertEquals(renamedColumnName, namingConventionFilter.renameColumn(originalColumnName, renameStrategy));
    }

    public static Stream<Arguments> when_given_column_name_should_convert_to_correct_format_based_on_strategy() {
        return Stream.of(
                arguments("column_number_1_name", Fixture.columnNameInCamelCase, CAMEL_CASE),
                arguments("COLUMN_NUMBER_1_NAME", Fixture.columnNameInCamelCase, CAMEL_CASE),
                arguments("column-number-1-name", Fixture.columnNameInCamelCase, CAMEL_CASE),
                arguments("column number 1 name", Fixture.columnNameInCamelCase, CAMEL_CASE),
                arguments("coLuMn nuMber 1 NamE", Fixture.columnNameInCamelCase, CAMEL_CASE),
                arguments("#coLuMn_nuMber 1-nAme", "#columnNumber1Name", CAMEL_CASE),
                arguments("Filesize (compressed, in bytes)", "filesizeCompressedInBytes", CAMEL_CASE),

                arguments("column_number_1_name", Fixture.columnNameInPascalCase, PASCAL_CASE),
                arguments("COLUMN_NUMBER_1_NAME", Fixture.columnNameInPascalCase, PASCAL_CASE),
                arguments("column-number-1-name", Fixture.columnNameInPascalCase, PASCAL_CASE),
                arguments("column number 1 name", Fixture.columnNameInPascalCase, PASCAL_CASE),
                arguments("coLuMn nuMber 1 NamE", Fixture.columnNameInPascalCase, PASCAL_CASE),
                arguments("#coLuMn_nuMber 1-nAme", "#columnNumber1Name", PASCAL_CASE),
                arguments("Filesize (compressed, in bytes)", "FilesizeCompressedInBytes", PASCAL_CASE),

                arguments("column_number_1_name", Fixture.columnNameInSnakeCase, SNAKE_CASE),
                arguments("COLUMN_NUMBER_1_NAME", Fixture.columnNameInSnakeCase, SNAKE_CASE),
                arguments("column-number-1-name", Fixture.columnNameInSnakeCase, SNAKE_CASE),
                arguments("column number 1 name", Fixture.columnNameInSnakeCase, SNAKE_CASE),
                arguments("coLuMn nuMber 1 NamE", Fixture.columnNameInSnakeCase, SNAKE_CASE),
                arguments("#coLuMn_nuMber 1-nAme", "#column_number1_name", SNAKE_CASE),
                arguments("Filesize (compressed, in bytes)", "filesize_compressed_in_bytes", SNAKE_CASE)
        );
    }

    @ParameterizedTest
    @MethodSource
    void when_given_config_value_should_retrieve_the_correct_rename_strategy_enum(String configValue, NamingConvention renameStrategy) {
        Assertions.assertEquals(renameStrategy, NamingConvention.getByConfigValue(configValue));
    }

    public static Stream<Arguments> when_given_config_value_should_retrieve_the_correct_rename_strategy_enum() {
        return Stream.of(
                arguments(CAMEL_CASE.getConfigValue(), CAMEL_CASE),
                arguments(PASCAL_CASE.getConfigValue(), PASCAL_CASE),
                arguments(SNAKE_CASE.getConfigValue(), SNAKE_CASE));
    }

    @Test
    void when_given_config_value_does_not_exist_should_throw_config_exception() {
        assertThrows(ConfigException.class, () -> NamingConvention.getByConfigValue("unknown strategy"));
    }

    @ParameterizedTest
    @MethodSource
    void when_apply_method_is_called_then_record_should_contain_renamed_columns(String configValue, String[] renamedColumnNames) {
        NamingConventionFilter renameStrategyFilter = new NamingConventionFilter();
        renameStrategyFilter.configure(of(
                CSV_DEFAULT_COLUMN_RENAME_STRATEGY_CONFIG, configValue,
                CSV_DEFAULT_COLUMN_RENAME_DELIMITER_CONFIG, "_"));
        RecordsIterable<TypedStruct> recordsIterable = renameStrategyFilter.apply(mock(FilterContext.class), buildInputRecord(), false);

        List<String> renamedFieldNames = extractRenamedFiledNames(recordsIterable);

        Assertions.assertArrayEquals(renamedColumnNames, renamedFieldNames.toArray());
    }

    public static Stream<Arguments> when_apply_method_is_called_then_record_should_contain_renamed_columns() {
        return Stream.of(
                arguments(CAMEL_CASE.getConfigValue(), Fixture.renamedColumnsCamelCase),
                arguments(PASCAL_CASE.getConfigValue(), Fixture.renamedColumnsPascalCase),
                arguments(SNAKE_CASE.getConfigValue(), Fixture.renamedColumnsSnakeCase));
    }

    private static List<String> extractRenamedFiledNames(RecordsIterable<TypedStruct> recordsIterable) {
        TypedStruct renamedRecords = recordsIterable.stream().findFirst().orElseThrow();
        List<TypedField> fields = renamedRecords.schema().fields();

        return fields.stream().map(TypedField::name).collect(Collectors.toList());
    }

    private static TypedStruct buildInputRecord() {
        TypedStruct record = TypedStruct.create();

        record.put(Fixture.idField, "value");
        record.put(Fixture.requestIdField, "value");
        record.put(Fixture.timesField, "value");
        record.put(Fixture.referrerField, "value");
        record.put(Fixture.urlField, "value");
        record.put(Fixture.searchEngineField, "value");
        return record;
    }

    interface Fixture {
        String columnNameInCamelCase = "columnNumber1Name";
        String columnNameInPascalCase = "ColumnNumber1Name";
        String columnNameInSnakeCase = "column_number1_name";

        String idField = "#SID";
        String requestIdField = "REQUEST_ID";
        String timesField = "TIMES";
        String referrerField = "REFERRER";
        String urlField = "URL";
        String searchEngineField = "SEARCH_ENGINE";

        String idFieldRenamedCamelCase = "#sid";
        String requestIdFieldRenamedCamelCase = "requestId";
        String timesFieldRenamedCamelCase = "times";
        String referrerFieldRenamedCamelCase = "referrer";
        String urlFieldRenamedCamelCase = "url";
        String searchEngineFieldRenamedCamelCase = "searchEngine";

        String[] renamedColumnsCamelCase = { idFieldRenamedCamelCase, referrerFieldRenamedCamelCase, requestIdFieldRenamedCamelCase,
                searchEngineFieldRenamedCamelCase, timesFieldRenamedCamelCase, urlFieldRenamedCamelCase
        };

        String idFieldRenamedPascalCase = "#sid";
        String requestIdFieldRenamedPascalCase = "RequestId";
        String timesFieldRenamedPascalCase = "Times";
        String referrerFieldRenamedPascalCase = "Referrer";
        String urlFieldRenamedPascalCase = "Url";
        String searchEngineFieldRenamedPascalCase = "SearchEngine";


        String[] renamedColumnsPascalCase = {idFieldRenamedPascalCase, referrerFieldRenamedPascalCase, requestIdFieldRenamedPascalCase,
                searchEngineFieldRenamedPascalCase, timesFieldRenamedPascalCase, urlFieldRenamedPascalCase
        };

        String idFieldRenamedSnakeCase = "#sid";
        String requestIdFieldRenamedSnakeCase = "request_id";
        String timesFieldRenamedSnakeCase = "times";
        String referrerFieldRenamedSnakeCase = "referrer";
        String urlFieldRenamedSnakeCase = "url";
        String searchEngineFieldRenamedSnakeCase = "search_engine";

        String[] renamedColumnsSnakeCase = {idFieldRenamedSnakeCase, referrerFieldRenamedSnakeCase, requestIdFieldRenamedSnakeCase,
                searchEngineFieldRenamedSnakeCase, timesFieldRenamedSnakeCase, urlFieldRenamedSnakeCase};
    }
}