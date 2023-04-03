/*
 * Copyright 2021 StreamThoughts.
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
package io.streamthoughts.kafka.connect.filepulse.filter;

import io.streamthoughts.kafka.connect.filepulse.config.XmlToJsonFilterConfig;
import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.data.TypedValue;
import io.streamthoughts.kafka.connect.filepulse.internal.StringUtils;
import io.streamthoughts.kafka.connect.filepulse.reader.RecordsIterable;
import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.config.ConfigDef;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.XML;
import org.json.XMLParserConfiguration;

public class XmlToJsonFilter extends AbstractMergeRecordFilter<XmlToJsonFilter> {

    private XmlToJsonFilterConfig config;

    private XMLParserConfiguration xmlParserConfiguration;

    /**
     * {@inheritDoc}
     */
    @Override
    public ConfigDef configDef() {
        return XmlToJsonFilterConfig.configDef();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(final Map<String, ?> props) {
        super.configure(props);
        config = new XmlToJsonFilterConfig(props);

        xmlParserConfiguration = new XMLParserConfiguration()
                .withKeepStrings(config.getXmlParserKeepStrings())
                .withcDataTagName(config.getCDataTagName());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected RecordsIterable<TypedStruct> apply(final FilterContext context,
                                                 final TypedStruct record) throws FilterException {

        final TypedValue value = checkIsNotNull(record.get(config.source()));
        switch (value.type()) {
            case STRING:
                final String xml = value.getString();
                if (StringUtils.isBlank(value.getString())) {
                    return RecordsIterable.empty();
                }
                return xmlToJson(new StringReader(xml));
            case BYTES:
                byte[] bytes = value.getBytes();
                if (bytes.length == 0) {
                    return RecordsIterable.empty();
                }
                return xmlToJson(new InputStreamReader(new ByteArrayInputStream(bytes), config.charset()));
            default:
                throw new FilterException(
                        "Invalid field '" + config.source() + "' was passed through the " +
                        "connector's configuration'. " +
                        "Cannot parse field of type '" + value.type() + "' to XML."
                );
        }
    }

    private RecordsIterable<TypedStruct> xmlToJson(final Reader xml) {
        try {
            final JSONObject xmlJSONObj = XML.toJSONObject(xml, xmlParserConfiguration);
            final String jsonString = xmlJSONObj.toString(0);
            return RecordsIterable.of(TypedStruct.create().put(config.source(), jsonString));
        } catch (JSONException e) {
            throw new FilterException("Failed to parse and convert XML document into JSON object", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Set<String> overwrite() {
        return Collections.singleton(config.source());
    }

    private TypedValue checkIsNotNull(final TypedValue value) {
        if (value.isNull()) {
            throw new FilterException(
                    "Invalid field '" + config.source() + "' was passed through the connector's configuration'. " +
                            "Cannot parse null or empty value to XML."
            );
        }
        return value;
    }
}
