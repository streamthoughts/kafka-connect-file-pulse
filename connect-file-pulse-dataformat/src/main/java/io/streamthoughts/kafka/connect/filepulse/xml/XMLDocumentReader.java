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
package io.streamthoughts.kafka.connect.filepulse.xml;

import java.io.IOException;
import java.io.InputStream;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import net.sf.saxon.lib.NamespaceConstant;
import org.w3c.dom.Document;
import org.xml.sax.ErrorHandler;
import org.xml.sax.SAXException;

public class XMLDocumentReader {

    static {
        System.setProperty(
                "javax.xml.xpath.XPathFactory:" + NamespaceConstant.OBJECT_MODEL_SAXON,
                "net.sf.saxon.xpath.XPathFactoryImpl"
        );
    }

    private final DocumentBuilder documentBuilder;

    /**
     * Creates a new {@link XMLDocumentReader} instance.
     *
     * @param isNamespaceAware Specifies that the parser produced by this code will provide support for XML namespaces.
     * @param isValidating Specifies that the parser produced by this code will validate documents as they are parsed.
     */
    public XMLDocumentReader(final boolean isNamespaceAware,
                             final boolean isValidating) {
        try {
            final DocumentBuilderFactory builderFactory = DocumentBuilderFactory.newInstance();
            builderFactory.setIgnoringElementContentWhitespace(true);
            builderFactory.setIgnoringComments(true);
            builderFactory.setNamespaceAware(isNamespaceAware);
            builderFactory.setValidating(isValidating);
            this.documentBuilder = builderFactory.newDocumentBuilder();
        } catch (ParserConfigurationException e) {
            throw new IllegalStateException(e);
        }
    }

    public Document parse(final InputStream inputStream) throws IOException, SAXException {
        return parse(inputStream, null);
    }

    public Document parse(final InputStream inputStream,
                          final ErrorHandler errorHandler) throws IOException, SAXException {
        try {
            if (errorHandler != null) {
                documentBuilder.setErrorHandler(errorHandler);
            }
            return documentBuilder.parse(inputStream);
        } finally {
            documentBuilder.reset();
            documentBuilder.setErrorHandler(null);
        }
    }
}
