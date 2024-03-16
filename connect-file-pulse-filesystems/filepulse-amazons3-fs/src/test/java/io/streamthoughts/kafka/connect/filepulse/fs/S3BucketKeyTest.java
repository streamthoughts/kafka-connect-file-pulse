/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.fs;

import org.junit.Assert;
import org.junit.Test;

public class S3BucketKeyTest {

    @Test
    public void should_return_object_name_given_key_with_prefix() {
        S3BucketKey key = new S3BucketKey("bucket", "/path/to/object/text.csv");
        Assert.assertEquals("text.csv", key.objectName());
    }
}