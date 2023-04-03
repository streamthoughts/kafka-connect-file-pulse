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
package io.streamthoughts.kafka.connect.filepulse.expression.accessor;

import io.streamthoughts.kafka.connect.filepulse.expression.EvaluationContext;
import io.streamthoughts.kafka.connect.filepulse.expression.converter.Converters;
import java.lang.reflect.Method;
import java.util.Objects;

public class ReflectivePropertyAccessor implements PropertyAccessor {

    private static final String GETTER_PREFIX = "get";
    private static final String SETTER_PREFIX = "set";
    private static final String DOT = ".";

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
    public Object read(final EvaluationContext context,
                       final Object target,
                       final String name) throws AccessException {
        Objects.requireNonNull(target, "target cannot be null");
        Objects.requireNonNull(name, "name cannot be null");

        Class<?> type = (target instanceof Class) ? (Class<?>) target : target.getClass();

        try {
            Method method = findGetterMethodForProperty(type, name);
            if (method != null || (method = findAccessMethodForProperty(type, name)) != null) {
                method.setAccessible(true);
                return method.invoke(target);
            }

            if (isDotPropertyAccessPath(name)) {
                String[] split = name.split("\\.", 2);
                Method rootMethod = findGetterMethodForProperty(type, split[0]);
                if (rootMethod != null || (rootMethod = findAccessMethodForProperty(type, name)) != null) {
                    rootMethod.setAccessible(true);
                    Object rootObject = rootMethod.invoke(target);
                    return new PropertyAccessors(context).readPropertyValue(rootObject, split[1]);
                }
            }
        } catch (Exception e) {
            throw new AccessException(e.getMessage());
        }
        throw new AccessException(
            String.format(
                "Cannot found getter method for attribute '%s' on class '%s'",
                name,
                target.getClass()
            )
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void write(final EvaluationContext context,
                      final Object target,
                      final String name,
                      final Object newValue) throws AccessException {
        Objects.requireNonNull(target, "target cannot be null");
        Objects.requireNonNull(name, "name cannot be null");

        Class<?> type = target instanceof Class ? (Class<?>) target : target.getClass();

        try {
            Method method = findSetterMethodForProperty(type, name);
            if (method != null ) {
                method.setAccessible(true);
                Class<?> expectedSetterType = method.getParameterTypes()[0];
                Object converted = Converters.converts(context.getPropertyConverter(), newValue, expectedSetterType);
                method.invoke(target, converted);
                return;
            }
        } catch (Exception e) {
            throw new AccessException(
                String.format(
                    "Cannot set property '%s' to '%s' on target type %s : %s",
                    name,
                    target.getClass().getSimpleName(),
                    target.getClass().getSimpleName(),
                    e.getMessage()) );
        }
        throw new AccessException(
            String.format(
                "Cannot found setter method for attribute '%s' on class '%s'",
                name,
                target.getClass().getCanonicalName())
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean canWrite(final EvaluationContext context,
                            final Object target,
                            final String name) throws AccessException {
        return true;
    }

    private Method findGetterMethodForProperty(final Class<?> target, final String name) {
        for (Method m : target.getMethods()) {
            String methodName = m.getName();
            if (methodName.equals(GETTER_PREFIX + getMethodSuffixForProperty(name))) {
                return m;
            }
        }
        return null;
    }

    private Method findSetterMethodForProperty(final Class<?> target, final String name) {
        for (Method m : target.getDeclaredMethods()) {
            String methodName = m.getName();
            if (methodName.equals(SETTER_PREFIX + getMethodSuffixForProperty(name))) {
                return m;
            }
        }
        return null;
    }

    private Method findAccessMethodForProperty(final Class<?> target, final String name) {
        for (Method m : target.getMethods()) {
            String methodName = m.getName();
            if (methodName.equals(name)) {
                return m;
            }
        }
        return null;
    }

    private String getMethodSuffixForProperty(final String name) {
        return Character.toUpperCase(name.charAt(0)) + name.substring(1);
    }

    private static boolean isDotPropertyAccessPath(final String name) {
        return name.contains(DOT);
    }
}