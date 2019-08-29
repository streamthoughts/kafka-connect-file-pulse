/*
 * Copyright 2019 StreamThoughts.
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
package io.streamthoughts.kafka.connect.filepulse;

import io.restassured.http.ContentType;
import io.restassured.parsing.Parser;
import io.restassured.response.Response;
import io.streamthoughts.kafka.connect.filepulse.source.FilePulseSourceConnector;
import org.junit.Assert;
import org.junit.Test;

import static io.restassured.RestAssured.*;
import java.util.List;
import java.util.Map;

public class FilePulseIT extends AbstractKafkaConnectTest {

    @Test
    public void testConnectorPluginsIsLoaded() {
        final Response response = doGetRequest("http://" + getConnectWorker() + "/connector-plugins");
        List<Map<String, String>> plugins = response.jsonPath().getList("$");

        for (final Map<String, String> plugin : plugins) {
            String connectorClass = plugin.get("class");
            if (connectorClass.equals(FilePulseSourceConnector.class.getCanonicalName())) {
                Assert.assertEquals(Version.getVersion(), plugin.get("version"));
                Assert.assertEquals("source", plugin.get("type"));
                return;
            }
        }
        Assert.fail("Connector plugins not loaded : " + FilePulseSourceConnector.class.getCanonicalName());
    }

    public static Response doGetRequest(final String endpoint) {
        defaultParser = Parser.JSON;
        return
                given().headers("Content-Type", ContentType.JSON, "Accept", ContentType.JSON).
                        when().get(endpoint).
                        then().contentType(ContentType.JSON).extract().response();
    }
}
