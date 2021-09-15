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
package io.streamthoughts.kafka.connect.filepulse.expression.function;

import io.streamthoughts.kafka.connect.filepulse.expression.Expression;
import io.streamthoughts.kafka.connect.filepulse.expression.ExpressionException;
import io.streamthoughts.kafka.connect.filepulse.expression.function.impl.And;
import io.streamthoughts.kafka.connect.filepulse.expression.function.impl.Concat;
import io.streamthoughts.kafka.connect.filepulse.expression.function.impl.ConcatWs;
import io.streamthoughts.kafka.connect.filepulse.expression.function.impl.Converts;
import io.streamthoughts.kafka.connect.filepulse.expression.function.impl.EndsWith;
import io.streamthoughts.kafka.connect.filepulse.expression.function.impl.Equals;
import io.streamthoughts.kafka.connect.filepulse.expression.function.impl.Exists;
import io.streamthoughts.kafka.connect.filepulse.expression.function.impl.ExtractArray;
import io.streamthoughts.kafka.connect.filepulse.expression.function.impl.Hash;
import io.streamthoughts.kafka.connect.filepulse.expression.function.impl.If;
import io.streamthoughts.kafka.connect.filepulse.expression.function.impl.IsNull;
import io.streamthoughts.kafka.connect.filepulse.expression.function.impl.Lowercase;
import io.streamthoughts.kafka.connect.filepulse.expression.function.impl.Md5;
import io.streamthoughts.kafka.connect.filepulse.expression.function.impl.Matches;
import io.streamthoughts.kafka.connect.filepulse.expression.function.impl.Nlv;
import io.streamthoughts.kafka.connect.filepulse.expression.function.impl.Length;
import io.streamthoughts.kafka.connect.filepulse.expression.function.impl.Or;
import io.streamthoughts.kafka.connect.filepulse.expression.function.impl.ReplaceAll;
import io.streamthoughts.kafka.connect.filepulse.expression.function.impl.Split;
import io.streamthoughts.kafka.connect.filepulse.expression.function.impl.StartsWith;
import io.streamthoughts.kafka.connect.filepulse.expression.function.impl.Trim;
import io.streamthoughts.kafka.connect.filepulse.expression.function.impl.UnixTimestamp;
import io.streamthoughts.kafka.connect.filepulse.expression.function.impl.Uppercase;
import io.streamthoughts.kafka.connect.filepulse.expression.function.impl.Uuid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class ExpressionFunctionExecutors {

    private static final Logger LOG = LoggerFactory.getLogger(ExpressionFunctionExecutors.class);

    private static final ExpressionFunctionExecutors INSTANCE = new ExpressionFunctionExecutors();

    public static ExpressionFunctionExecutor resolve(final String functionName, final Expression[] arguments) {
        return INSTANCE.make(functionName, arguments);
    }

    private final Map<String, ExpressionFunction> functions = new HashMap<>();


    /**
     * Creates a new {@link ExpressionFunctionExecutors} instance.
     */
    private ExpressionFunctionExecutors() {
        // List of built-in expression functions to register.
        register(new Lowercase());
        register(new Uppercase());
        register(new Converts());
        register(new Length());
        register(new Nlv());
        register(new ExtractArray());
        register(new StartsWith());
        register(new EndsWith());
        register(new IsNull());
        register(new Matches());
        register(new Exists());
        register(new Equals());
        register(new Trim());
        register(new ReplaceAll());
        register(new Uuid());
        register(new Concat());
        register(new ConcatWs());
        register(new Hash());
        register(new Md5());
        register(new Split());
        register(new UnixTimestamp());
        register(new If());
        register(new And());
        register(new Or());
    }

    @SuppressWarnings("unchecked")
    private ExpressionFunctionExecutor make(final String functionName, final Expression[] arguments) {
        Objects.requireNonNull(functionName, "functionName cannot be null");
        boolean exists = functions.containsKey(functionName);
        if (!exists) {
            throw new ExpressionException("Invalid expression, function does not exist '" + functionName + "'. "
                    + "Valid functions are : " + functions.keySet());
        }

        ExpressionFunction function = functions.get(functionName);
        final Arguments prepared = function.prepare(arguments);

        if (!prepared.valid()) {
            final String errorMessages = prepared.buildErrorMessage();
            throw new ExpressionException("Invalid arguments for function '" + functionName + "' : " + errorMessages);
        }
        return new ExpressionFunctionExecutor(functionName, function, prepared);
    }

    public void register(final ExpressionFunction function) {
        Objects.requireNonNull(function, "'function' should not be null");
        LOG.info("Registered built-in expression function '{}'", function.name() );
        functions.put(function.name(), function);
    }
}
