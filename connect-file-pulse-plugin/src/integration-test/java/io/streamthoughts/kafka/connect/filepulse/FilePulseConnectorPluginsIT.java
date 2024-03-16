/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse;

import static io.restassured.RestAssured.defaultParser;
import static io.restassured.RestAssured.given;

import io.restassured.http.ContentType;
import io.restassured.parsing.Parser;
import io.restassured.response.Response;
import io.streamthoughts.kafka.connect.filepulse.source.FilePulseSourceConnector;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("integration")
public class FilePulseConnectorPluginsIT extends AbstractKafkaConnectTest {

    @Test
    public void should_load_connector_plugin() {

        List<Map<String, String>> plugins;
        final Response response = doGetRequest("http://" + getConnectWorker() + "/connector-plugins");
        plugins = response.jsonPath().getList("$");

        System.out.println(plugins);
        for (final Map<String, String> plugin : plugins) {
            String connectorClass = plugin.get("class");
            if (connectorClass.equals(FilePulseSourceConnector.class.getCanonicalName())) {
                Assertions.assertEquals(Version.getVersion(), plugin.get("version"));
                Assertions.assertEquals("source", plugin.get("type"));
                return;
            }
        }
        Assertions.fail("Connector plugins not loaded : " + FilePulseSourceConnector.class.getCanonicalName());
    }

    public static Response doGetRequest(final String endpoint) {
        defaultParser = Parser.JSON;
        return
                given().headers("Content-Type", ContentType.JSON, "Accept", ContentType.JSON).
                        when().get(endpoint).
                        then().contentType(ContentType.JSON).extract().response();
    }
}
