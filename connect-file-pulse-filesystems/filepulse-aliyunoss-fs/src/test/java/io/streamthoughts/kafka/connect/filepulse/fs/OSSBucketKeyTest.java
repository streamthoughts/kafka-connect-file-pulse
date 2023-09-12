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
