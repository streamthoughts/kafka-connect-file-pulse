/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
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
