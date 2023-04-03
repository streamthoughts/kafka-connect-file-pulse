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
import io.streamthoughts.kafka.connect.filepulse.expression.function.collections.ExtractArray;
import io.streamthoughts.kafka.connect.filepulse.expression.function.conditions.And;
import io.streamthoughts.kafka.connect.filepulse.expression.function.conditions.Equals;
import io.streamthoughts.kafka.connect.filepulse.expression.function.conditions.GreaterThan;
import io.streamthoughts.kafka.connect.filepulse.expression.function.conditions.If;
import io.streamthoughts.kafka.connect.filepulse.expression.function.conditions.LessThan;
import io.streamthoughts.kafka.connect.filepulse.expression.function.conditions.Not;
import io.streamthoughts.kafka.connect.filepulse.expression.function.conditions.Or;
import io.streamthoughts.kafka.connect.filepulse.expression.function.datetime.TimestampDiff;
import io.streamthoughts.kafka.connect.filepulse.expression.function.datetime.ToTimestamp;
import io.streamthoughts.kafka.connect.filepulse.expression.function.datetime.UnixTimestamp;
import io.streamthoughts.kafka.connect.filepulse.expression.function.objects.Converts;
import io.streamthoughts.kafka.connect.filepulse.expression.function.objects.Exists;
import io.streamthoughts.kafka.connect.filepulse.expression.function.objects.ExtractStructField;
import io.streamthoughts.kafka.connect.filepulse.expression.function.objects.IsNull;
import io.streamthoughts.kafka.connect.filepulse.expression.function.objects.Nlv;
import io.streamthoughts.kafka.connect.filepulse.expression.function.strings.Concat;
import io.streamthoughts.kafka.connect.filepulse.expression.function.strings.ConcatWs;
import io.streamthoughts.kafka.connect.filepulse.expression.function.strings.EndsWith;
import io.streamthoughts.kafka.connect.filepulse.expression.function.strings.FromBytes;
import io.streamthoughts.kafka.connect.filepulse.expression.function.strings.Hash;
import io.streamthoughts.kafka.connect.filepulse.expression.function.strings.IsEmpty;
import io.streamthoughts.kafka.connect.filepulse.expression.function.strings.Length;
import io.streamthoughts.kafka.connect.filepulse.expression.function.strings.Lowercase;
import io.streamthoughts.kafka.connect.filepulse.expression.function.strings.Matches;
import io.streamthoughts.kafka.connect.filepulse.expression.function.strings.Md5;
import io.streamthoughts.kafka.connect.filepulse.expression.function.strings.ParseUrl;
import io.streamthoughts.kafka.connect.filepulse.expression.function.strings.ReplaceAll;
import io.streamthoughts.kafka.connect.filepulse.expression.function.strings.Split;
import io.streamthoughts.kafka.connect.filepulse.expression.function.strings.StartsWith;
import io.streamthoughts.kafka.connect.filepulse.expression.function.strings.Trim;
import io.streamthoughts.kafka.connect.filepulse.expression.function.strings.Uppercase;
import io.streamthoughts.kafka.connect.filepulse.expression.function.strings.Uuid;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExpressionFunctionExecutors {

    private static final Logger LOG = LoggerFactory.getLogger(ExpressionFunctionExecutors.class);

    public static final ExpressionFunctionExecutors INSTANCE = new ExpressionFunctionExecutors();

    public static ExpressionFunctionExecutor resolve(final String functionName,
                                                     final Expression[] arguments) {
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
        register(new IsEmpty());
        register(new ReplaceAll());
        register(new Uuid());
        register(new Concat());
        register(new ConcatWs());
        register(new Hash());
        register(new Md5());
        register(new Split());
        register(new UnixTimestamp());
        register(new ToTimestamp());
        register(new TimestampDiff());
        register(new If());
        register(new And());
        register(new Or());
        register(new GreaterThan());
        register(new LessThan());
        register(new Not());
        register(new ParseUrl());
        register(new ExtractStructField());
        register(new FromBytes());
    }

    private ExpressionFunctionExecutor make(final String functionName, final Expression[] arguments) {
        Objects.requireNonNull(functionName, "functionName cannot be null");
        boolean exists = functions.containsKey(functionName);
        if (!exists) {
            throw new ExpressionException(
                    "Invalid expression, function does not exist '" + functionName + "'. "
                    + "Valid functions are : " + functions.keySet()
            );
        }

        final ExpressionFunction function = functions.get(functionName);

        final ExpressionFunction.Instance instance = function.get();

        final Arguments prepared;
        try {
            prepared = instance.prepare(arguments);
        } catch (Exception e) {
            throw new ExpressionException("Failed to prepare function '" + functionName + "': " + e.getMessage());
        }

        return new ExpressionFunctionExecutor(functionName, instance, prepared);

    }

    public void register(final ExpressionFunction function) {
        Objects.requireNonNull(function, "'function' should not be null");
        LOG.info("Registered built-in expression function '{}'", function.name() );
        functions.put(function.name(), function);
    }
}
