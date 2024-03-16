/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.fs;

import java.net.URI;
import org.junit.Assert;
import org.junit.Test;

public class OSSBucketKeyTest {

    @Test
    public void should_return_object_name_given_key_with_prefix() {
        OSSBucketKey key = new OSSBucketKey("bucket", "/path/to/object/text.csv");
        Assert.assertEquals("text.csv", key.objectName());
    }


    @Test
    public void should_return_object_name_given_key_with_url() {
        OSSBucketKey source = new OSSBucketKey("bucket", "/path/to/object/text.csv");
        URI uri = source.toURI();
        OSSBucketKey dest = OSSBucketKey.fromURI(uri);
        Assert.assertEquals("text.csv", dest.objectName());
    }
}
