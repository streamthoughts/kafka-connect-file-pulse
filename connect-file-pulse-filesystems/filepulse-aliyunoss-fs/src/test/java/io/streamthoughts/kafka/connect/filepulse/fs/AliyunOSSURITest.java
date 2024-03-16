/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.fs;

import io.streamthoughts.kafka.connect.filepulse.fs.utils.AliyunOSSURI;
import java.net.URI;
import org.junit.Assert;
import org.junit.Test;

public class AliyunOSSURITest {

    @Test
    public void testAll() {
        OSSBucketKey source = new OSSBucketKey("bucket", "/path/to/object/text.csv");
        URI uri = source.toURI();
        AliyunOSSURI aliyunOSSURI = new AliyunOSSURI(uri);
        Assert.assertEquals("bucket", aliyunOSSURI.getBucket());
        Assert.assertEquals("/path/to/object/text.csv", aliyunOSSURI.getKey());
    }

}
