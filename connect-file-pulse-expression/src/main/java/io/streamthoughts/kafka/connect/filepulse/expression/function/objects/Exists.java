/*
 * Copyright 2019-2021 StreamThoughts.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.streamthoughts.kafka.connect.filepulse.expression.function.objects;

import io.streamthoughts.kafka.connect.filepulse.data.Type;
import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.data.TypedValue;
import io.streamthoughts.kafka.connect.filepulse.expression.Expression;
import io.streamthoughts.kafka.connect.filepulse.expression.ExpressionException;
import io.streamthoughts.kafka.connect.filepulse.expression.function.AbstractExpressionFunctionInstance;
import io.streamthoughts.kafka.connect.filepulse.expression.function.Arguments;
import io.streamthoughts.kafka.connect.filepulse.expression.function.EvaluatedExecutionContext;
import io.streamthoughts.kafka.connect.filepulse.expression.function.ExpressionFunction;

public class Exists implements ExpressionFunction {

    private static final String OBJECT_ARG = "object_expr";
    private static final String FIELD_ARG = "field_expr";

    /**
     * {@inheritDoc}
     */
    @Override
    public Instance get() {
        return new AbstractExpressionFunctionInstance() {

            private String syntax() {
                return String.format("syntax %s(<%s>, <%s>)", name(), OBJECT_ARG, FIELD_ARG);
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public Arguments prepare(final Expression[] args) {
                  if (args.length < 2) {
                    throw new ExpressionException("Missing required arguments: " + syntax());
                }

                return Arguments.of(OBJECT_ARG, args[0], FIELD_ARG, args[1]);
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public TypedValue invoke(final EvaluatedExecutionContext context) throws ExpressionException {
                final TypedValue object = context.get(OBJECT_ARG);
                final TypedValue path = context.get(FIELD_ARG);

                if (object.type() != Type.STRUCT && object.type() != Type.NULL) {
                    throw new ExpressionException("Expected type [STRUCT|NULL], was " + object.type());
                }

                if (object.type() == Type.NULL) return TypedValue.bool(false);

                final TypedStruct struct = object.getStruct();
                final boolean exists = struct.exists(path.getString());
                return TypedValue.bool(exists);
            }
        };
    }
}
