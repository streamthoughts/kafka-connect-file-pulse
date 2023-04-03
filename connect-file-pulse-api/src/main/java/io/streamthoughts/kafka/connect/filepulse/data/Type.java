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
package io.streamthoughts.kafka.connect.filepulse.data;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;

public enum Type {

    // This is a special type used to deal with NULL object.
    NULL(null, null) {
        @Override
        public Short convert(final Object o) {
            throw new UnsupportedOperationException("Cannot convert an object to type NULL");
        }
        @Override
        protected boolean isInternal() {
            return true;
        }
    },

    SHORT(Collections.singletonList(Short.class), Schema.Type.INT16) {
        @Override
        public Short convert(final Object o) {
            return TypedValue.any(o).getShort();
        }
    },

    INTEGER(Collections.singletonList(Integer.class), Schema.Type.INT32) {

        @Override
        public Integer convert(final Object o) {
            return TypedValue.any(o).getInt();
        }
    },

    LONG(Collections.singletonList(Long.class), Schema.Type.INT64) {
        @Override
        public Long convert(final Object o) {
            return TypedValue.any(o).getLong();
        }
    },

    FLOAT(Collections.singletonList(Float.class), Schema.Type.FLOAT32) {
        @Override
        public Float convert(final Object o) {
            return TypedValue.any(o).getFloat();
        }
    },

    DOUBLE(Collections.singletonList(Double.class), Schema.Type.FLOAT64) {
        @Override
        public Double convert(final Object o) {
            return TypedValue.any(o).getDouble();
        }
    },

    BOOLEAN(Collections.singletonList(Boolean.class), Schema.Type.BOOLEAN) {
        @Override
        public Boolean convert(final Object o) {
            return TypedValue.any(o).getBool();
        }
    },

    STRING(Collections.singletonList(String.class), Schema.Type.STRING) {
        @Override
        public String convert(final Object o) {
            return TypedValue.any(o).getString();
        }
    },

    ARRAY(Collections.singletonList(Collection.class), Schema.Type.ARRAY) {
        @Override
        public Collection convert(final Object o) {
            return TypedValue.any(o).getArray();
        }

    },

    MAP(Collections.singletonList(Map.class), Schema.Type.MAP) {
        @Override
        public Map<String, ?> convert(final Object o) {
            return TypedValue.any(o).getMap();
        }
    },

    STRUCT(Collections.singletonList(TypedStruct.class), Schema.Type.STRUCT) {
        @Override
        public TypedStruct convert(Object o) {
            return TypedValue.any(o).getStruct();
        }
    },

    BYTES(Collections.emptyList(), Schema.Type.BYTES) {
        @Override
        public byte[] convert(Object o) {
            return TypedValue.any(o).getBytes();
        }
    };
    
    private final static Map<Class<?>, Type> JAVA_CLASS_TYPES = new HashMap<>();

    static {
        for (Type type : Type.values()) {
            if (!type.isInternal()) {
                for (Class<?> typeClass : type.classes) {
                    JAVA_CLASS_TYPES.put(typeClass, type);
                }
            }
        }
    }

    private final Schema.Type schemaType;

    private final Collection<Class<?>> classes;

    /**
     * Creates a new {@link Type} instance.
     *
     * @param schemaType the connect schema type.
     */
    Type(final Collection<Class<?>> classes, final Schema.Type schemaType) {
        this.classes = classes;
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

    /**
     * Converts the specified object to this type.
     *
     * @param o the object to be converted.
     * @return  the converted object.
     */
    public abstract Object convert(final Object o) ;

    /**
     * Checks whether this is type is internal.
     * Internal types cannot be resolved from a class or string name.
     *
     * @return {@code false}.
     */
    protected boolean isInternal() {
        return false;
    }

    public boolean isPrimitive() {
        switch (this) {
            case FLOAT:
            case DOUBLE:
            case INTEGER:
            case LONG:
            case BOOLEAN:
            case STRING:
            case BYTES:
                return true;
            default:
        }
        return false;
    }

    public boolean isNumber() {
        switch (this) {
            case FLOAT:
            case DOUBLE:
            case INTEGER:
            case LONG:
                return true;
            default:
        }
        return false;
    }

    public static Type forName(final String name) {
        for (Type type : Type.values()) {
            if (!type.isInternal()
                && (type.name().equals(name) || type.schemaType.name().equals(name))) {
                return type;
            }
        }
        throw new DataException("Cannot find corresponding Type for name : " + name);
    }

    public static Type forClass(final Class<?> cls) {
        synchronized (JAVA_CLASS_TYPES) {
            Type type = JAVA_CLASS_TYPES.get(cls);
            if (type != null)
                return type;

            // Since the lookup only checks the class, we need to also try
            for (Map.Entry<Class<?>, Type> entry : JAVA_CLASS_TYPES.entrySet()) {
                try {
                    cls.asSubclass(entry.getKey());
                    // Cache this for subsequent lookups
                    JAVA_CLASS_TYPES.put(cls, entry.getValue());
                    return entry.getValue();
                } catch (ClassCastException e) {
                    // Expected, ignore
                }
            }
        }
        return null;
    }

    public static Type forConnectSchemaType(final Schema.Type schemaType) {
        for (Type type : Type.values()) {
            if (type != Type.NULL && type.schemaType.equals(schemaType)) {
                return type;
            }
        }
        throw new DataException("Cannot find corresponding Type for Schema.Type : " + schemaType.getName());
    }
}
