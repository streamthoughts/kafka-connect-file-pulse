/*
 * Copyright 2022 StreamThoughts.
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
package io.streamthoughts.kafka.connect.filepulse.expression.function.strings;

import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.data.TypedValue;
import io.streamthoughts.kafka.connect.filepulse.expression.Expression;
import io.streamthoughts.kafka.connect.filepulse.expression.ExpressionException;
import io.streamthoughts.kafka.connect.filepulse.expression.ValueExpression;
import io.streamthoughts.kafka.connect.filepulse.expression.function.AbstractExpressionFunctionInstance;
import io.streamthoughts.kafka.connect.filepulse.expression.function.Arguments;
import io.streamthoughts.kafka.connect.filepulse.expression.function.EvaluatedExecutionContext;
import io.streamthoughts.kafka.connect.filepulse.expression.function.ExpressionFunction;
import io.streamthoughts.kafka.connect.filepulse.internal.StringUtils;
import java.net.URI;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link ExpressionFunction} to parse a valid field-value {@link URL}/{@link URI} and return
 * a {@link TypedStruct} object consisting of all the components (fragment, host, path, port, query, scheme).
 */
public class ParseUrl implements ExpressionFunction {

    private static final String FIELD_ARG = "field_expr";
    private static final String PERMISSIVE_ARG = "permissive";

    private static final Logger LOG = LoggerFactory.getLogger(ParseUrl.class);

    private static final Pattern QUERY_PATTERN = Pattern.compile("([^&=]+)(=?)([^&]+)?");

    public static final String FRAGMENT_FIELD = "fragment";
    public static final String HOST_FIELD = "host";
    public static final String PATH_FIELD = "path";
    public static final String PORT_FIELD = "port";
    public static final String QUERY_FIELD = "query";
    public static final String SCHEME_FIELD = "scheme";
    public static final String USER_INFO_FIELD = "userInfo";
    public static final String ERROR_FIELD = "error";

    public static final String PARAMETERS_FIELD = "parameters";

    /**
     * {@inheritDoc}
     */
    @Override
    public ExpressionFunction.Instance get() {
        return new Instance(name());
    }

    static class Instance extends AbstractExpressionFunctionInstance {

        private final String name;

        private boolean permissive;

        public Instance(final String name) {
            this.name = Objects.requireNonNull(name, "'name' should not be null");
        }

        private String syntax() {
            return String.format("syntax %s(<%s>, [<%s>])", name, FIELD_ARG, PERMISSIVE_ARG);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Arguments prepare(final Expression[] args) {
            if (args.length > 2) {
                throw new ExpressionException("Too many arguments: " + syntax());
            }
            if (args.length < 1) {
                throw new ExpressionException("Missing required arguments: " + syntax());
            }

            permissive = args.length == 2 ? ((ValueExpression) args[1]).value().getBool() : false;

            return Arguments.of(FIELD_ARG, args[0]);
        }

        @Override
        public TypedValue invoke(final EvaluatedExecutionContext context) throws ExpressionException {

            final String stringValue = context.get(0).value();
            try {
                URI uri = URI.create(stringValue);
                if (uri.getScheme() == null) {
                    return rethrowOrGetPermissiveResult(stringValue,
                            "Could not parse URL: scheme not specified");
                }
                return TypedValue.struct(TypedStruct.create()
                        .put(FRAGMENT_FIELD, uri.getFragment())
                        .put(HOST_FIELD, uri.getHost())
                        .put(USER_INFO_FIELD, uri.getUserInfo())
                        .put(PATH_FIELD, getPath(uri))
                        .put(PORT_FIELD, uri.getPort() == -1 ? null : uri.getPort())
                        .put(QUERY_FIELD, uri.getQuery())
                        .put(SCHEME_FIELD, uri.getScheme())
                        .put(PARAMETERS_FIELD, parseQueryParams(uri.getRawQuery()))
                );
            } catch (IllegalArgumentException e) {
                return rethrowOrGetPermissiveResult(stringValue, e.getLocalizedMessage());
            }
        }

        private static String getPath(final URI uri) {
            return Optional.ofNullable(uri.getPath()).orElse(uri.getSchemeSpecificPart());
        }

        private TypedValue rethrowOrGetPermissiveResult(final String stringValue,
                                                        final String errorMessage) {
            if (!permissive) {
                throw new ExpressionException(errorMessage);
            }

            LOG.warn("Error parsing URL from value {}. Error:  {}", stringValue,errorMessage);
            return TypedValue.struct(TypedStruct.create()
                    .put(ERROR_FIELD, errorMessage)
            );
        }

        protected Map<String, List<String>> parseQueryParams(final String query) {
            Map<String, List<String>> queryParams = null;
            if (query != null) {
                queryParams = new LinkedHashMap<>();
                Matcher matcher = QUERY_PATTERN.matcher(query);
                while (matcher.find()) {
                    String name = decodeQueryParam(matcher.group(1));
                    String eq = matcher.group(2);
                    String value = matcher.group(3);
                    value = value != null ? decodeQueryParam(value) : StringUtils.isNotBlank(eq) ? "" : null;
                    queryParams.computeIfAbsent(name, k -> new LinkedList<>()).add(value);
                }
            }
            return queryParams;
        }

        private String decodeQueryParam(final String value) {
            return URLDecoder.decode(value, StandardCharsets.UTF_8);
        }

    }
}
