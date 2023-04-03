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
package io.streamthoughts.kafka.connect.filepulse.fs.filter;

import static org.junit.Assert.*;

import io.streamthoughts.kafka.connect.filepulse.source.FileObjectMeta;
import java.net.URI;
import java.util.Map;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

public class SizeFileListFilterTest {

    @Test
    public void should_not_filter_file_given_default_limit_size_bytes() {
        SizeFileListFilter filter = new SizeFileListFilter();
        filter.configure(Map.of());
        Assert.assertTrue(filter.test(newFileObjectMeta(0)));
    }

    @Test
    public void should_filter_file_given_minimum_size_bytes_superior_to_zero() {
        SizeFileListFilter filter = new SizeFileListFilter();
        filter.configure(Map.of(
                SizeFileListFilter.FILE_MINIMUM_SIZE_MS_CONFIG, 1
        ));
        Assert.assertFalse(filter.test(newFileObjectMeta(0)));
    }

    @Test
    public void should_filter_file_given_maximum_size_bytes() {
        SizeFileListFilter filter = new SizeFileListFilter();
        filter.configure(Map.of(
                SizeFileListFilter.FILE_MAXIMUM_SIZE_MS_CONFIG, 0
        ));
        Assert.assertFalse(filter.test(newFileObjectMeta(1)));
    }

    @NotNull
    private FileObjectMeta newFileObjectMeta(final long contentLength) {
        return new FileObjectMeta() {
            @Override
            public URI uri() {
                return null;
            }

            @Override
            public String name() {
                return null;
            }

            @Override
            public Long contentLength() {
                return contentLength;
            }

            @Override
            public Long lastModified() {
                return null;
            }

            @Override
            public ContentDigest contentDigest() {
                return null;
            }

            @Override
            public Map<String, Object> userDefinedMetadata() {
                return null;
            }
        };
    }

}