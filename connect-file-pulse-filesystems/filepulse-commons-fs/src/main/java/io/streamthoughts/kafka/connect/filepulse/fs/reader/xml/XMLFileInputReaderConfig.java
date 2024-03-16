/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.fs.reader.xml;

import static org.apache.kafka.common.config.ConfigDef.ValidString.in;

import io.streamthoughts.kafka.connect.filepulse.xml.XMLCommonConfig;
import java.util.Map;
import javax.xml.xpath.XPathConstants;
import org.apache.kafka.common.config.ConfigDef;

/**
 *
 */
public class XMLFileInputReaderConfig extends XMLCommonConfig {

    private static final String GROUP = "XML_INPUT_FILE_READER";

    private static final String READER_KEY_PREFIX = "reader.";

    public static final String XPATH_QUERY_CONFIG = READER_KEY_PREFIX + "xpath.expression";
    private static final String XPATH_QUERY_DOC = "The XPath expression used extract data from XML input files (default: '/')";

    public static final String XPATH_RESULT_TYPE_CONFIG = READER_KEY_PREFIX + "xpath.result.type";
    private static final String XPATH_RESULT_TYPE_DOC = "The expected result type for the XPath expression in [NODESET, STRING]";

    /**
     * Creates a new {@link XMLFileInputReaderConfig} instance.
     *
     * @param originals the reader configuration.
     */
    public XMLFileInputReaderConfig(final Map<String, ?> originals) {
        super(READER_KEY_PREFIX, configDef(), originals);
    }

    public String xpathQuery() {
        return getString(XPATH_QUERY_CONFIG);
    }

    public String resultType() {
        return getString(XPATH_RESULT_TYPE_CONFIG);
    }

    public static String withKeyPrefix(final String key) {
        return READER_KEY_PREFIX + key;
    }

    public static ConfigDef configDef() {
        final ConfigDef additional = new ConfigDef()
                .define(
                        XPATH_RESULT_TYPE_CONFIG,
                        ConfigDef.Type.STRING,
                        XPathConstants.NODESET.getLocalPart(),
                        in(XPathConstants.NODESET.getLocalPart(), XPathConstants.STRING.getLocalPart()),
                        ConfigDef.Importance.HIGH,
                        XPATH_RESULT_TYPE_DOC
                )
                .define(
                        XPATH_QUERY_CONFIG,
                        ConfigDef.Type.STRING,
                        "/",
                        ConfigDef.Importance.HIGH,
                        XPATH_QUERY_DOC
                );

        return new ConfigDef(XMLCommonConfig.buildConfigDefWith(
                GROUP,
                READER_KEY_PREFIX,
                additional.configKeys().values())
        );

    }
}