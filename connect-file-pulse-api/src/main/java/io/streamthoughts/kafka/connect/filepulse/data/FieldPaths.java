/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.data;

import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class FieldPaths {

    private final Set<FieldPath> paths;

    public static FieldPaths empty() {
        return new FieldPaths(Collections.emptySet());
    }

    public static FieldPaths from(final Collection<String> paths) {
        return new FieldPaths(paths.stream()
            .map(FieldPath::new)
            .collect(Collectors.toSet()));
    }

    public FieldPaths next(final String currentField) {
        return new FieldPaths(paths.stream()
            .map(p -> p.forwardIfOrNull(currentField))
            .filter(Objects::nonNull)
            .collect(Collectors.toSet())
        );
    }

    private FieldPaths(final Set<FieldPath> paths) {
        this.paths = paths;
    }

    public boolean anyMatches(final String fieldName) {
        return paths.stream().anyMatch(it -> it.matches(fieldName));
    }

    public static class FieldPath {
        private final String path;
        private final String field;
        private final String remaining;

        FieldPath(final String path) {
            this.path = path;
            if (path.contains(".")) {
                String[] split = path.split("\\.", 2);
                field = split[0];
                remaining = split[1];
            } else {
                field = path;
                remaining = null;
            }
        }

        public boolean matches(final String field) {
            return path.equals(field);
        }

        private FieldPath forwardIfOrNull(final String field) {
            if (!this.field.equals(field))
                return null;
            if (remaining == null)
                return null;
            return new FieldPath(remaining);
        }
    }
}
