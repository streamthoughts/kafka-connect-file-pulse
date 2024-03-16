/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.fs.filter;

import io.streamthoughts.kafka.connect.filepulse.source.GenericFileObjectMeta;
import java.time.Instant;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

public class LastModifiedFileListFilterTest {

    @Test
    public void should_filter_given_object_file_not_matching_minimum_age() {
        final LastModifiedFileListFilter filter = new LastModifiedFileListFilter();
        filter.configure(Map.of(LastModifiedFileListFilter.FILE_MINIMUM_AGE_MS_CONFIG, 5000));

        Assert.assertTrue(filter.test(getFileObjectMeta(Instant.now().minusMillis(10_000L))));
        Assert.assertFalse(filter.test(getFileObjectMeta(Instant.now())));
    }

    @Test
    public void should_filter_given_object_file_not_matching_maximum_age() {
        final LastModifiedFileListFilter filter = new LastModifiedFileListFilter();
        filter.configure(Map.of(LastModifiedFileListFilter.FILE_MAXIMUM_AGE_MS_CONFIG, 10_000));

        Assert.assertTrue(filter.test(getFileObjectMeta(Instant.now().minusMillis(5_000L))));
        Assert.assertFalse(filter.test(getFileObjectMeta(Instant.now().minusMillis(15_000L))));
    }

    private GenericFileObjectMeta getFileObjectMeta(final Instant lastModified) {
        return new GenericFileObjectMeta
                .Builder()
                .withLastModified(lastModified)
                .build();
    }
}