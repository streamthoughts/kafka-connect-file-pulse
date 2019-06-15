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
package io.streamthoughts.kafka.connect.filepulse.data;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

public enum Type {

    SHORT(Schema.Type.INT16) {
        /**
         * {@inheritDoc}
         */
        @Override
        public Short convert(final Object o) {
            return TypeValue.of(o).getShort();
        }
    },
    INTEGER(Schema.Type.INT32) {
        /**
         * {@inheritDoc}
         */
        @Override
        public Integer convert(final Object o) {
            return TypeValue.of(o).getInt();
        }
    },
    LONG(Schema.Type.INT64) {
        /**
         * {@inheritDoc}
         */
        @Override
        public Long convert(final Object o) {
            return TypeValue.of(o).getLong();
        }
    },
    FLOAT(Schema.Type.FLOAT32) {
        /**
         * {@inheritDoc}
         */
        @Override
        public Float convert(final Object o) {
            return TypeValue.of(o).getFloat();
        }
    },
    DOUBLE(Schema.Type.FLOAT64) {
        /**
         * {@inheritDoc}
         */
        @Override
        public Double convert(final Object o) {
            return TypeValue.of(o).getDouble();
        }
    },
    BOOLEAN(Schema.Type.BOOLEAN) {
        /**
         * {@inheritDoc}
         */
        @Override
        public Boolean convert(final Object o) {
            return TypeValue.of(o).getBool();
        }
    },
    STRING(Schema.Type.STRING) {
        /**
         * {@inheritDoc}
         */
        @Override
        public String convert(final Object o) {
            return TypeValue.of(o).getString();
        }
    };

    private Schema.Type  schemaType;

    /**
     * Creates a new {@link Type} instance.
     *
     * @param schemaType the connect schema type.
     */
    Type(final Schema.Type schemaType) {
        this.schemaType = schemaType;
    }

    /**
     * Returns the {@link Schema.Type} instance for this {@link Type}.
     *
     * @return a {@link Schema.Type} instance.
     */
    public Schema.Type schemaType() {
        return schemaType;
    }

    public SchemaBuilder schema() {
        return new SchemaBuilder(schemaType);
    }

    /**
     * Converts the specified object to this type.
     *
     * @param o the object to be converted.
     * @return  the converted object.
     */
    public abstract Object convert(final Object o) ;

    public static Type fromSchemaType(final Schema.Type schemaType) {
        for (Type type : Type.values()) {
            if (type.schemaType.equals(schemaType)) {
                return type;
            }
        }
        throw new DataException("Cannot find corresponding Type for Schema.Type : " + schemaType.getName());
    }
}
