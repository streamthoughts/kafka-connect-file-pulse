/*
 * Copyright 2019-2020 StreamThoughts.
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
package io.streamthoughts.kafka.connect.filepulse.pattern;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static io.streamthoughts.kafka.connect.filepulse.pattern.GrokSchemaBuilder.buildSchemaForGrok;

public class GrokSchemaBuilderTest {

    private static final String GROK_PATTERN = "(?<timestamp>%{YEAR}-%{MONTHNUM}-%{MONTHDAY} %{TIME}) %{LOGLEVEL:level} %{GREEDYDATA:message}";

    @Test
    public void test() {
        final GrokPatternCompiler compiler = new GrokPatternCompiler(new GrokPatternResolver(), true);
        final GrokMatcher matcher = compiler.compile(GROK_PATTERN);
        Schema schema = buildSchemaForGrok(Collections.singletonList(matcher));
        List<Field> fields = schema.schema().fields();
        Assert.assertEquals(3, fields.size());
        Assert.assertEquals("timestamp", fields.get(0).name());
        Assert.assertEquals("level", fields.get(1).name());
        Assert.assertEquals("message", fields.get(2).name());
    }
}