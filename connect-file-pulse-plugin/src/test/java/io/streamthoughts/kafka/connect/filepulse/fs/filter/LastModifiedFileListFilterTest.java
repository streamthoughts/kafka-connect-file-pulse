/*
 * Copyright 2019-2021 StreamThoughts.
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