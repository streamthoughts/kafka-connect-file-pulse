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
package io.streamthoughts.kafka.connect.filepulse.expression;

import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class SimpleEvaluationContext implements EvaluationContext {

    protected final Map<String, SchemaAndValue> variables;

    /**
     * Creates a new {@link SimpleEvaluationContext} instance.
     * @param variables
     */
    protected SimpleEvaluationContext(final Map<String, SchemaAndValue> variables) {
        this.variables = variables;
        set("$env", asStruct(System.getenv()));
        set("$props", asStruct(System.getProperties()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean has(final String name) {
        return variables.containsKey(name);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SchemaAndValue get(final String name) {
        return variables.get(name);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void set(final String name, final SchemaAndValue value) {
        variables.put(name, value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<String> variables() {
        return variables.keySet();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, SchemaAndValue> values() {
        return variables;
    }

    private static SchemaAndValue asStruct(final Properties props) {
        final SchemaBuilder schema = SchemaBuilder.struct();
        for (final String propName : props.stringPropertyNames()) {
            schema.field(propName, SchemaBuilder.string()).optional();
        }
        Struct struct = new Struct(schema);
        for (final String propName : props.stringPropertyNames()) {
            struct.put(propName, props.getProperty(propName));
        }
        return new SchemaAndValue(schema, struct);
    }

    private static SchemaAndValue asStruct(final Map<String, ?> props) {
        final SchemaBuilder schema = SchemaBuilder.struct();
        for (final String propName : props.keySet()) {
            schema.field(propName, SchemaBuilder.string()).optional();
        }
        Struct struct = new Struct(schema);
        for (final Map.Entry<String, ?> kv : props.entrySet()) {
            struct.put(kv.getKey(), kv.getValue());
        }
        return new SchemaAndValue(schema, struct);
    }
}
