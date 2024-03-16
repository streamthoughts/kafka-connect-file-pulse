/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.internal;

import java.util.Arrays;
import java.util.Locale;
import java.util.MissingResourceException;

public class LocaleUtils {

    public static Locale parse(String localeStr) {
        String separator = separatorFor(localeStr);
        final String[] parts = localeStr.split(separator, -1);

        final Locale locale = buildLocale(parts);

        checkIsValidISOLanguage(parts, locale);
        checkIsValidISOCountry(parts, locale);

        return locale;
    }

    private static String separatorFor(final String localeStr) {
        char sep = '-'; // expected character.
        for (int i = 0; i < localeStr.length(); ++i) {
            final char c = localeStr.charAt(i);
            if (c == '-') {
                break;
            } else if (c == '_') {
                sep = '_';
                break;
            }
        }
        return String.valueOf(sep);
    }

    private static void checkIsValidISOCountry(final String[] parts, final Locale locale) {
        try {
            locale.getISO3Country();
        } catch (MissingResourceException e) {
            throw new IllegalArgumentException("Cannot build Locale for unknown country : " + parts[0], e);
        }
    }

    private static void checkIsValidISOLanguage(final String[] parts, final Locale locale) {
        try {
            locale.getISO3Language();
        } catch (MissingResourceException e) {
            throw new IllegalArgumentException("Cannot build Locale for unknown language: " + parts[1], e);
        }
    }

    private static Locale buildLocale(final String[] parts) {
        switch (parts.length) {
            case 3:
                return new Locale(parts[0], parts[1], parts[2]);
            case 2:
                return new Locale(parts[0], parts[1]);
            case 1:
                if ("ROOT".equalsIgnoreCase(parts[0])) {
                    return Locale.ROOT;
                }
                return new Locale(parts[0]);
            default:
                throw new IllegalArgumentException(
                    String.format(
                        "Locales can have at most 3 parts but got %d : %s",
                        parts.length, Arrays.asList(parts))
                );
        }
    }
}
