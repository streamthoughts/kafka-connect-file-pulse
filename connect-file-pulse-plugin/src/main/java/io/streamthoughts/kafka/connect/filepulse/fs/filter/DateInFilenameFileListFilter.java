/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.fs.filter;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.time.format.DateTimeFormatter.ofPattern;

import io.streamthoughts.kafka.connect.filepulse.fs.PredicateFileListFilter;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectMeta;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DateInFilenameFileListFilter extends PredicateFileListFilter {

    private final static String GROUP = "DateInFilenameFileListFilter";

    private static final Logger LOG = LoggerFactory.getLogger(DateInFilenameFileListFilter.class);

    static final String FILE_FILTER_DATE_REGEX_EXTRACTOR_PATTERN_CONFIG =
            "file.filter.date.regex.extractor.pattern";

    private static final String FILE_FILTER_DATE_REGEX_EXTRACTOR_PATTERN_DOC =
            "The regex pattern used to extract the date from the filename";

    static final String FILE_FILTER_DATE_FORMATTER_PATTERN_CONFIG =
            "file.filter.date.formatter.pattern";

    private static final String FILE_FILTER_DATE_FORMATTER_PATTERN_DOC =
            "The formatter pattern used to format the date extracted from the filename";

    static final String FILE_FILTER_DATE_MIN_DATE_CONFIG =
            "file.filter.date.min.date";

    private static final String FILE_FILTER_DATE_MIN_DATE_DOC =
            "The minimum date that the date extracted from the filename should match (filenameDate >= minDate)";

    static final String FILE_FILTER_DATE_MAX_DATE_CONFIG =
            "file.filter.date.max.date";

    private static final String FILE_FILTER_DATE_MAX_DATE_DOC =
            "The maximum date that the date extracted from the filename should match (filenameDate < maxDate)";

    private Pattern pattern;

    DateTimeFormatter dateFormatter;

    LocalDate minDate;

    LocalDate maxDate;

    @Override
    public void configure(Map<String, ?> config) {
        AbstractConfig abstractConfig = new AbstractConfig(getConfigDef(), config, false);
        String pattern = abstractConfig.getString(FILE_FILTER_DATE_REGEX_EXTRACTOR_PATTERN_CONFIG);
        if (pattern == null) {
            throw new ConfigException("missing configuration: " + FILE_FILTER_DATE_REGEX_EXTRACTOR_PATTERN_CONFIG);
        }

        setPattern(pattern);

        dateFormatter = ofPattern(abstractConfig.getString(FILE_FILTER_DATE_FORMATTER_PATTERN_CONFIG));

        String strMinDate = abstractConfig.getString(FILE_FILTER_DATE_MIN_DATE_CONFIG);
        String strMaxDate = abstractConfig.getString(FILE_FILTER_DATE_MAX_DATE_CONFIG);

        if (strMinDate == null && strMaxDate == null) {
            throw new ConfigException("At least one of " + FILE_FILTER_DATE_MIN_DATE_CONFIG + " or " +
                    FILE_FILTER_DATE_MAX_DATE_CONFIG + " should be specified");
        }

        if (strMinDate != null) {
            minDate = DateValidator.parseDate(FILE_FILTER_DATE_MIN_DATE_CONFIG, strMinDate);
        }
        if (strMaxDate != null) {
            maxDate = DateValidator.parseDate(FILE_FILTER_DATE_MAX_DATE_CONFIG, strMaxDate);
        }
    }

    private static ConfigDef getConfigDef() {
        DateValidator dateValidator = new DateValidator();
        int groupCounter = 0;
        return new ConfigDef()
                .define(FILE_FILTER_DATE_REGEX_EXTRACTOR_PATTERN_CONFIG,
                        ConfigDef.Type.STRING,
                        ConfigDef.Importance.HIGH,
                        FILE_FILTER_DATE_REGEX_EXTRACTOR_PATTERN_DOC,
                        GROUP,
                        groupCounter++,
                        ConfigDef.Width.NONE,
                        FILE_FILTER_DATE_REGEX_EXTRACTOR_PATTERN_CONFIG)
                .define(FILE_FILTER_DATE_FORMATTER_PATTERN_CONFIG,
                        ConfigDef.Type.STRING,
                        "yyyy-MM-dd",
                        ConfigDef.Importance.HIGH,
                        FILE_FILTER_DATE_FORMATTER_PATTERN_DOC,
                        GROUP,
                        groupCounter++,
                        ConfigDef.Width.NONE,
                        FILE_FILTER_DATE_FORMATTER_PATTERN_CONFIG)
                .define(FILE_FILTER_DATE_MIN_DATE_CONFIG,
                        ConfigDef.Type.STRING,
                        null,
                        dateValidator,
                        ConfigDef.Importance.LOW,
                        FILE_FILTER_DATE_MIN_DATE_DOC,
                        GROUP,
                        groupCounter++,
                        ConfigDef.Width.NONE,
                        FILE_FILTER_DATE_MIN_DATE_CONFIG)
                .define(FILE_FILTER_DATE_MAX_DATE_CONFIG,
                        ConfigDef.Type.STRING,
                        null,
                        dateValidator,
                        ConfigDef.Importance.LOW,
                        FILE_FILTER_DATE_MAX_DATE_DOC,
                        GROUP,
                        groupCounter,
                        ConfigDef.Width.NONE,
                        FILE_FILTER_DATE_MAX_DATE_CONFIG)
                ;
    }

    @Override
    public boolean test(FileObjectMeta meta) {
        if (meta == null) {
            return false;
        }

        Matcher matcher = pattern.matcher(meta.stringURI());
        boolean matched = false;

        if (matcher.find() && matcher.groupCount() > 0) {
            matched = true;

            String strDate = matcher.group(1);
            LocalDate filenameDate = LocalDate.parse(strDate, dateFormatter);

            if (minDate != null) {
                matched = !filenameDate.isBefore(minDate);
            }
            if (maxDate != null) {
                matched &= filenameDate.isBefore(maxDate);
            }

            if (!matched) {
                String msg = String.format("Date '%s' is not between boundaries [%s, %s)",
                        filenameDate, minDate == null ? "-inf" : minDate, maxDate == null ? "+inf" : maxDate);
                LOG.debug(msg);
            }
        } else {
            LOG.debug("Cannot extract date from '" + meta.stringURI() + "' using regexp '" + pattern.toString() + "'");
        }

        return matched;
    }

    private void setPattern(String pattern) {
        this.pattern = Pattern.compile(pattern);
    }

    public static class DateValidator implements ConfigDef.Validator {

        @Override
        public void ensureValid(String name, Object value) {
            String date = (String)value;
            if (date != null) {
                if (date.isEmpty()) {
                    throw new ConfigException(name, value, "Date must be non-empty");
                }

                parseDate(name, date);
            }
        }

        public static LocalDate parseDate(String configKey, String date) {
            try {
                return LocalDate.parse(date, ISO_LOCAL_DATE);
            } catch (DateTimeParseException e) {
                throw new ConfigException(configKey, date, "Cannot parse date: " + date);
            }
        }
    }
}
