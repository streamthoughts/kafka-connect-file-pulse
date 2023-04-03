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

import static java.util.Objects.requireNonNull;

import java.util.Objects;

public abstract class AbstractExpression implements Expression {

    private final String originalExpression;

    /**
     * Creates a new {@link AbstractExpression} instance.
     *
     * @param originalExpression    the original string expression.
     */
    public AbstractExpression(final String originalExpression) {
        this.originalExpression = requireNonNull(originalExpression, "originalExpression cannot be null");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AbstractExpression)) return false;
        AbstractExpression that = (AbstractExpression) o;
        return Objects.equals(originalExpression, that.originalExpression);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return Objects.hash(originalExpression);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String originalExpression() {
        return originalExpression;
    }
}
