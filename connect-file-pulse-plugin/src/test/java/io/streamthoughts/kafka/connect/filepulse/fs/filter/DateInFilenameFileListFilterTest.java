/*
 * Copyright 2023 StreamThoughts.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.streamthoughts.kafka.connect.filepulse.fs.filter;

import static io.streamthoughts.kafka.connect.filepulse.fs.filter.DateInFilenameFileListFilter.FILE_FILTER_DATE_MAX_DATE_CONFIG;
import static io.streamthoughts.kafka.connect.filepulse.fs.filter.DateInFilenameFileListFilter.FILE_FILTER_DATE_MIN_DATE_CONFIG;
import static io.streamthoughts.kafka.connect.filepulse.fs.filter.DateInFilenameFileListFilter.FILE_FILTER_DATE_REGEX_EXTRACTOR_PATTERN_CONFIG;
import static io.streamthoughts.kafka.connect.filepulse.fs.filter.DateInFilenameFileListFilterTest.Fixture.dateExtractorRegex;
import static io.streamthoughts.kafka.connect.filepulse.fs.filter.DateInFilenameFileListFilterTest.Fixture.fileBetweenCutoffDates;
import static io.streamthoughts.kafka.connect.filepulse.fs.filter.DateInFilenameFileListFilterTest.Fixture.fileDateAfterMaxCutoffDate;
import static io.streamthoughts.kafka.connect.filepulse.fs.filter.DateInFilenameFileListFilterTest.Fixture.fileDateBeforeMinCutoffDate;
import static io.streamthoughts.kafka.connect.filepulse.fs.filter.DateInFilenameFileListFilterTest.Fixture.fileDateEqualsMaxCutoffDate;
import static io.streamthoughts.kafka.connect.filepulse.fs.filter.DateInFilenameFileListFilterTest.Fixture.fileDateEqualsMinCutoffDate;
import static io.streamthoughts.kafka.connect.filepulse.fs.filter.DateInFilenameFileListFilterTest.Fixture.fileWithoutDateInName;
import static io.streamthoughts.kafka.connect.filepulse.fs.filter.DateInFilenameFileListFilterTest.Fixture.invalidCutoffDate;
import static io.streamthoughts.kafka.connect.filepulse.fs.filter.DateInFilenameFileListFilterTest.Fixture.maxCutoffDate;
import static io.streamthoughts.kafka.connect.filepulse.fs.filter.DateInFilenameFileListFilterTest.Fixture.minCutoffDate;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import io.streamthoughts.kafka.connect.filepulse.source.FileObjectMeta;
import io.streamthoughts.kafka.connect.filepulse.source.GenericFileObjectMeta;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class DateInFilenameFileListFilterTest {

    @Test
    void when_pattern_empty_configure_should_throw_exception() {
        DateInFilenameFileListFilter filter = new DateInFilenameFileListFilter();
        ConfigException configException = assertThrows(ConfigException.class, () -> filter.configure(Map.of()));
        assertNull(configException.getCause());
        assertTrue(configException.getMessage().contains(FILE_FILTER_DATE_REGEX_EXTRACTOR_PATTERN_CONFIG));
    }

    @Test
    void when_date_min_invalid_configure_should_throw_exception() {
        Map<String, Object> configs =
                Map.of(FILE_FILTER_DATE_REGEX_EXTRACTOR_PATTERN_CONFIG, dateExtractorRegex,
                        FILE_FILTER_DATE_MIN_DATE_CONFIG, invalidCutoffDate);

        DateInFilenameFileListFilter filter = new DateInFilenameFileListFilter();
        ConfigException configException = assertThrows(ConfigException.class, () -> filter.configure(configs));
        assertNull(configException.getCause());
        assertTrue(configException.getMessage().contains(FILE_FILTER_DATE_MIN_DATE_CONFIG));
    }

    @Test
    void when_date_max_invalid_configure_should_throw_exception() {
        Map<String, Object> configs =
                Map.of(FILE_FILTER_DATE_REGEX_EXTRACTOR_PATTERN_CONFIG, dateExtractorRegex,
                        FILE_FILTER_DATE_MAX_DATE_CONFIG, invalidCutoffDate);

        DateInFilenameFileListFilter filter = new DateInFilenameFileListFilter();
        ConfigException configException = assertThrows(ConfigException.class, () -> filter.configure(configs));
        assertNull(configException.getCause());
        assertTrue(configException.getMessage().contains(FILE_FILTER_DATE_MAX_DATE_CONFIG));
    }

    @Test
    void when_neither_max_date_nor_min_date_provided_configure_should_throw_exception() {
        Map<String, Object> configs =
                Map.of(FILE_FILTER_DATE_REGEX_EXTRACTOR_PATTERN_CONFIG, dateExtractorRegex);

        DateInFilenameFileListFilter filter = new DateInFilenameFileListFilter();
        ConfigException configException = assertThrows(ConfigException.class, () -> filter.configure(configs));
        assertNull(configException.getCause());
        assertTrue(configException.getMessage().contains("At least one of"));
    }

    private DateInFilenameFileListFilter prepareFilter(String minCutoffDate, String maxCutoffDate) {
        Map<String, Object> configs = new HashMap<>();
        configs.put(FILE_FILTER_DATE_REGEX_EXTRACTOR_PATTERN_CONFIG, dateExtractorRegex);

        if (minCutoffDate != null) {
            configs.put(FILE_FILTER_DATE_MIN_DATE_CONFIG, minCutoffDate);
        }

        if (maxCutoffDate != null) {
            configs.put(FILE_FILTER_DATE_MAX_DATE_CONFIG, maxCutoffDate);
        }

        DateInFilenameFileListFilter filter = new DateInFilenameFileListFilter();
        filter.configure(configs);

        return filter;
    }

    @ParameterizedTest
    @MethodSource
    void when_called_test_should_return_expected_value(Boolean expected,
                                                       FileObjectMeta meta,
                                                       String minCutoffDate,
                                                       String maxCutoffDate) {

        DateInFilenameFileListFilter filter = prepareFilter(minCutoffDate, maxCutoffDate);
        assertEquals(expected, filter.test(meta));
    }

    public static Stream<Arguments> when_called_test_should_return_expected_value() {
        return Stream.of(
                arguments(false, fileWithoutDateInName, minCutoffDate, null),
                arguments(false, fileWithoutDateInName, null, maxCutoffDate),
                arguments(false, fileWithoutDateInName, minCutoffDate, maxCutoffDate),
                arguments(true,  fileDateEqualsMinCutoffDate, minCutoffDate, null),
                arguments(true,  fileDateEqualsMinCutoffDate, minCutoffDate, maxCutoffDate),
                arguments(false, fileDateBeforeMinCutoffDate, minCutoffDate, null),
                arguments(false, fileDateBeforeMinCutoffDate, minCutoffDate, maxCutoffDate),
                arguments(true,  fileBetweenCutoffDates, minCutoffDate, maxCutoffDate),
                arguments(false, fileDateEqualsMaxCutoffDate, null, maxCutoffDate),
                arguments(false, fileDateEqualsMaxCutoffDate, minCutoffDate, maxCutoffDate),
                arguments(false, fileDateAfterMaxCutoffDate, null, maxCutoffDate),
                arguments(false, fileDateAfterMaxCutoffDate, minCutoffDate, maxCutoffDate)
        );
    }

    interface Fixture {
        String minCutoffDate = "2022-04-01";
        String maxCutoffDate = "2022-12-31";
        String invalidCutoffDate = "2022-04-41";
        String dateExtractorRegex = "^.*_(\\d{4}-\\d{2}-\\d{2}).*$";

        FileObjectMeta fileWithoutDateInName = new GenericFileObjectMeta.Builder()
                .withName("f1.txt")
                .withUri(URI.create("/path/f1.txt"))
                .withContentLength(1024L)
                .withLastModified(System.currentTimeMillis())
                .build();

        FileObjectMeta fileDateEqualsMinCutoffDate = new GenericFileObjectMeta.Builder()
                .withName("f1_2022-04-01.txt")
                .withUri(URI.create("/path/f1_2022-04-01.txt"))
                .withContentLength(1024L)
                .withLastModified(System.currentTimeMillis())
                .build();
        FileObjectMeta fileDateBeforeMinCutoffDate = new GenericFileObjectMeta.Builder()
                .withName("f1_2022-03-01.txt")
                .withUri(URI.create("/path/f1_2022-03-01.txt"))
                .withContentLength(1024L)
                .withLastModified(System.currentTimeMillis())
                .build();
        FileObjectMeta fileDateAfterMaxCutoffDate = new GenericFileObjectMeta.Builder()
                .withName("f1_2023-01-01.txt")
                .withUri(URI.create("/path/f1_2023-01-01.txt"))
                .withContentLength(1024L)
                .withLastModified(System.currentTimeMillis())
                .build();

        FileObjectMeta fileDateEqualsMaxCutoffDate = new GenericFileObjectMeta.Builder()
                .withName("f1_2022-12-31.txt")
                .withUri(URI.create("/path/f1_2022-12-31.txt"))
                .withContentLength(1024L)
                .withLastModified(System.currentTimeMillis())
                .build();

        FileObjectMeta fileBetweenCutoffDates = new GenericFileObjectMeta.Builder()
                .withName("f1_2022-11-31.txt")
                .withUri(URI.create("/path/f1_2022-11-31.txt"))
                .withContentLength(1024L)
                .withLastModified(System.currentTimeMillis())
                .build();
    }
}