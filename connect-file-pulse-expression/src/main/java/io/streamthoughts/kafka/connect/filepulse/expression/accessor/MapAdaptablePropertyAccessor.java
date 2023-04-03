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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;

public class MapAdaptablePropertyAccessor implements PropertyAccessor {

    private static final String GET_METHOD_NAME = "get";
    private static final String PUT_METHOD_NAME = "put";
    private static final String DOT = ".";

    /**
     * {@inheritDoc}
     */
    @Override
    public Class<?>[] getSpecificTargetClasses() {
        return new Class[]{Map.class};
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
    @SuppressWarnings("unchecked")
    public Object read(final EvaluationContext context,
                       final Object target,
                       final String name) throws AccessException {
        Objects.requireNonNull(target, "target cannot be null");
        Objects.requireNonNull(name, "name cannot be null");

        Class<?> type = target instanceof Class ? (Class<?>) target : target.getClass();

        return Map.class.isAssignableFrom(type) ?
                readFromMapObject(context, (Map) target, name) :
                readFromMapAdaptableObject(context, target, name, type);
    }

    private Object readFromMapAdaptableObject(final EvaluationContext context,
                                              final Object target,
                                              final String key,
                                              final Class<?> type) {
        try {
            Method method = findGetterByKeyMethodForProperty(type);
            if (method != null && method.canAccess(target)) {
                final Object result = method.invoke(target, key);

                if (result != null) return result;

                // If result is NULL, then we need to check whether the given key represents a dotted path.
                if (isDotPropertyAccessPath(key)) {
                    String[] split = key.split("\\.", 2);
                    Object rootObject = method.invoke(target, split[0]);
                    if (rootObject != null) {
                        return new PropertyAccessors(context).readPropertyValue(rootObject, split[1]);
                    }
                }
            }
            throw new AccessException("Cannot access map property with key '" + key + "'. Entry does not exist.");
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new AccessException(e.getMessage());
        }
    }

    private Object readFromMapObject(final EvaluationContext context,
                                     final Map<String, Object> target,
                                     final String key) {

        if (target.containsKey(key)) return target.get(key);

        // If key does NOT exist, then we need to check whether the given key uses dotted-notation.
        if (isDotPropertyAccessPath(key)) {
            final String[] split = key.split("\\.", 2);
            final String rootKey = split[0];
            if (target.containsKey(rootKey)) {
                Object rootObject = target.get(rootKey);
                if (rootObject != null) {
                    return new PropertyAccessors(context).readPropertyValue(rootObject, split[1]);
                }
            }
        }
        throw new AccessException("Cannot access map property with key '" + key + "'. Entry does not exist.");
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
        Method method = findSetterByKeyMethodForProperty(type, name);

        if (method == null) {
            throw new AccessException(
                    String.format(
                            "Cannot found access method for attribute %s on class %s",
                            name,
                            target.getClass().getCanonicalName()
                    )
            );
        }

        try {
            method.invoke(target, name, newValue);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new AccessException(e.getMessage());
        }
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


    private Method findGetterByKeyMethodForProperty(final Class<?> target) {
        return findMethodForProperty(target, this::isAccessibleByKey);
    }


    private Method findMethodForProperty(final Class<?> target, final Predicate<Method> predicate) {
        Optional<Method> optional = Arrays.stream(target.getMethods())
                .filter(predicate)
                .findAny();
        return optional.orElse(null);
    }

    private Method findSetterByKeyMethodForProperty(final Class<?> target, final Object newValue) {
        return findMethodForProperty(target, method -> isSettableByKeyAssignableFrom(method, newValue));
    }

    private boolean isAccessibleByKey(final Method m) {
        String methodName = m.getName();
        if (methodName.equals(GET_METHOD_NAME) && m.getParameterCount() == 1) {
            Class<?>[] parameterTypes = m.getParameterTypes();
            return parameterTypes[0].isAssignableFrom(String.class);
        }
        return false;
    }

    private boolean isSettableByKeyAssignableFrom(final Method m, final Object newValue) {
        String methodName = m.getName();
        if (methodName.equals(PUT_METHOD_NAME) && m.getParameterCount() == 2) {
            Class<?>[] parameterTypes = m.getParameterTypes();
            boolean isStringKey = parameterTypes[0].isAssignableFrom(String.class);
            boolean isObjectValue = parameterTypes[1].isAssignableFrom(newValue.getClass());
            return isStringKey && isObjectValue;
        }
        return false;
    }

    private static boolean isDotPropertyAccessPath(final String name) {
        return name.contains(DOT);
    }
}