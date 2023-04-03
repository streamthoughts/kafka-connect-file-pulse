/*
 * Copyright 2021 StreamThoughts.
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
package io.streamthoughts.kafka.connect.filepulse.schema;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;

public class SchemaContext {

    final Map<String, CyclicSchemaWrapper> connectSchemaReferences = new HashMap<>();

    public Schema buildSchemaWithCyclicSchemaWrapper(final Schema connectSchema) {

        final String schemaName = connectSchema.name();
        if (schemaName != null) {
            CyclicSchemaWrapper wrapper;
            // We need to lookup if a Schema has already been referenced for that name.
            // So that we can, merge the new Schema with the reference that already exist.
            if (connectSchemaReferences.containsKey(schemaName)) {
                // Note, that we need to remove the reference because calling the merge method will
                // call that method again in a recursive way.
                wrapper = connectSchemaReferences.remove(schemaName);
                wrapper.schema(SchemaMerger.merge(wrapper.schema(), connectSchema, this));
            } else {
                wrapper = new CyclicSchemaWrapper(connectSchema);
                connectSchemaReferences.put(schemaName, wrapper);
            }
            return wrapper;
        }
        return connectSchema;
    }
}
