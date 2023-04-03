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
package io.streamthoughts.kafka.connect.filepulse.expression;

import io.streamthoughts.kafka.connect.filepulse.expression.accessor.PropertyAccessors;
import io.streamthoughts.kafka.connect.filepulse.expression.converter.Converters;
import io.streamthoughts.kafka.connect.filepulse.expression.converter.PropertyConverter;
import java.util.List;
import java.util.Objects;

public class PropertyExpression extends AbstractExpression {

    private final String rootObject;
    private final String attribute;

    /**
     * Creates a new {@link PropertyExpression} instance.
     *
     * @param original      the original expression.
     * @param rootObject    the root object.
     * @param attribute     the attribute path to access.
     */
    public PropertyExpression(final String original,
                              final String rootObject,
                              final String attribute) {
        super(original);
        this.rootObject = Objects.requireNonNull(rootObject, "rootObject cannot be null");;
        this.attribute = attribute;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object readValue(final EvaluationContext context) {
        return readValue(context, Object.class);
    }

    public String getRootObject() {
        return rootObject;
    }

    public String getAttribute() {
        return attribute;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T> T readValue(final EvaluationContext context, final Class<T> expectedType) {
        Objects.requireNonNull(context, " context cannot be null");
        Objects.requireNonNull(expectedType, " expectedType cannot be null");

        final PropertyAccessors accessors = new PropertyAccessors(context);

        Object returned = accessors.readPropertyValue(context.rootObject(), rootObject);

        if (attribute != null) {
            if (returned == null) {
                throw new ExpressionException(
                    "Cannot evaluate attribute expression '" + attribute + "', root object '"
                    + rootObject + "' returned null."
                );
            }
            returned = accessors.readPropertyValue(returned, attribute);
        }

        if (returned != null && expectedType.isAssignableFrom(returned.getClass())) {
            return (T)returned;
        }

        final List<PropertyConverter> converters = context.getPropertyConverter();
        return Converters.converts(converters, returned, expectedType);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeValue(final Object value, final EvaluationContext context) {
        final Object target = context.rootObject();

        final PropertyAccessors accessors = new PropertyAccessors(context);

        if (attribute == null) {
            accessors.writeValueForProperty(target, rootObject, value);
        } else {
            Object returned = accessors.readPropertyValue(target, rootObject);
            if (returned == null) {
                throw new ExpressionException(
                        "Cannot evaluate attribute expression '" + attribute + "', root object '"
                                + rootObject + "' returned null."
                );
            }
            accessors.writeValueForProperty(returned, attribute, value);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PropertyExpression)) return false;
        if (!super.equals(o)) return false;
        PropertyExpression that = (PropertyExpression) o;
        return Objects.equals(rootObject, that.rootObject) &&
                Objects.equals(attribute, that.attribute);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return Objects.hash(rootObject, attribute);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "[" +
                "originalExpression=" + originalExpression() +
                ", rootObject=" + rootObject +
                ", attribute=" + attribute +
                ']';
    }
}
