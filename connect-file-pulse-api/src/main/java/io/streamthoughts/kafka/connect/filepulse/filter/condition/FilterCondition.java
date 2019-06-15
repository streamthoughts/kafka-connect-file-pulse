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
package io.streamthoughts.kafka.connect.filepulse.filter.condition;

import io.streamthoughts.kafka.connect.filepulse.filter.FilterContext;
import io.streamthoughts.kafka.connect.filepulse.filter.FilterException;
import io.streamthoughts.kafka.connect.filepulse.reader.FileInputRecord;
import io.streamthoughts.kafka.connect.filepulse.source.FileInputData;

/**
 * Default interface to determine if a {@link io.streamthoughts.kafka.connect.filepulse.filter.RecordFilter}
 * must be applied.
 */
public interface FilterCondition {

    FilterCondition TRUE = (ctx, data) -> true;

    /**
     * Checks whether a filter can be applied on the specified {@link FileInputRecord}.
     *
     * @param context   the filter execution context.
     * @param record    the data to apply.
     *
     * @return {@code true} if filter can be applied.
     */
    boolean apply(final FilterContext context, final FileInputData record) throws FilterException;


    static FilterCondition revert(final FilterCondition condition) {
        return (context, record) -> ! condition.apply(context, record);
    }
}
