/*
 * Copyright 2023 StreamThoughts.
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
package io.streamthoughts.kafka.connect.filepulse.avro.internal;

import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;
import org.apache.avro.Schema;

public interface ConnectSchemaConverter {

    org.apache.kafka.connect.data.Schema toConnectSchema(Schema schema,
                                                         Options options,
                                                         CyclicContext context);

    /**
     * Class that holds the options for performing conversion.
     */
    class Options {
        private final boolean forceOptional;
        private final Object fieldDefaultValue;
        private final String docDefaultValue;

        public Options() {
            this(false, null, null);
        }

        public Options(boolean forceOptional, Object fieldDefaultValue, String docDefaultValue) {
            this.forceOptional = forceOptional;
            this.fieldDefaultValue = fieldDefaultValue;
            this.docDefaultValue = docDefaultValue;
        }

        public boolean forceOptional() {
            return forceOptional;
        }

        public Options forceOptional(boolean forceOptional) {
            return new Options(forceOptional, fieldDefaultValue, docDefaultValue);
        }

        public Options fieldDefaultValue(Object fieldDefaultValue) {
            return new Options(forceOptional, fieldDefaultValue, docDefaultValue);
        }

        public Object fieldDefaultValue() {
            return fieldDefaultValue;
        }

        public Options docDefaultValue(String docDefaultValue) {
            return new Options(forceOptional, fieldDefaultValue, docDefaultValue);
        }

        public String docDefaultValue() {
            return this.docDefaultValue;
        }
    }

    /**
     * Class that holds the context for performing conversion.
     */
    class CyclicContext {
        private final Map<Schema, CyclicSchemaWrapper> cycleReferences;
        private final Set<Schema> detectedCycles;

        /**
         * cycleReferences - map that holds connect Schema references to resolve cycles
         * detectedCycles - avro schemas that have been detected to have cycles
         */
        public CyclicContext() {
            this.cycleReferences = new IdentityHashMap<>();
            this.detectedCycles = new HashSet<>();
        }

        public Map<Schema, CyclicSchemaWrapper> cycleReferences() {
            return cycleReferences;
        }

        public Set<Schema> detectedCycles() {
            return detectedCycles;
        }
    }
}
