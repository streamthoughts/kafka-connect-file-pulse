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
package io.streamthoughts.kafka.connect.filepulse.expression.accessor;

import io.streamthoughts.kafka.connect.filepulse.expression.EvaluationContext;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;

import java.util.Objects;

public class StructFieldAccessor implements PropertyAccessor {

    /**
     * {@inheritDoc}
     */
    @Override
    public Class<?>[] getSpecificTargetClasses() {
        return new Class[]{Struct.class};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean canRead(final EvaluationContext context,
                           final Object target,
                           final String name) throws AccessException {
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SchemaAndValue read(final EvaluationContext context,
                               final Object target,
                               final String name) throws AccessException {
        Objects.requireNonNull(target, "target cannot be null");
        Objects.requireNonNull(name, "name cannot be null");

        final Struct struct = (Struct) target;

        Field field = struct.schema().field(name);
        if (field != null) {
            return new SchemaAndValue(field.schema(), struct.get(field));
        } else if (isDotPath(name)) {
            String[] split = name.split("\\.", 2);
            Object rootObject = read(context, target, split[0]);
            if (rootObject != null) {
                return read(context, rootObject, split[1]);
            }
        }

        throw new AccessException(
            "Can't access field '" + name + "' from Struct - field does not exist");
    }

    private boolean isDotPath(final String name) {
        return name.contains(".");
    }
}
