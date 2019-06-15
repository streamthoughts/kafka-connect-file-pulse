/*
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
package io.streamthoughts.kafka.connect.filepulse.filter;

import io.streamthoughts.kafka.connect.filepulse.source.FileInputData;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;

public class JSONFilterTest {

    private static final String JSON = "    {\"firstName\" : \"foo\", \"lastName\" : \"bar\"}";

    private static final FileInputData DATA = FileInputData.defaultStruct(JSON);

    private JSONFilter filter;

    @Before
    public void setUp() {
        filter = new JSONFilter();
        filter.configure(new HashMap<>());
    }

    @Test
    public void testGivenNoTarget() {
        List<FileInputData> output = this.filter.apply(null, DATA, false).collect();
        Assert.assertEquals(1, output.size());
    }

    @Test
    public void testGivenTarget() {
        List<FileInputData> output = this.filter.apply(null, DATA, false).collect();
        Assert.assertEquals(1, output.size());
    }
}