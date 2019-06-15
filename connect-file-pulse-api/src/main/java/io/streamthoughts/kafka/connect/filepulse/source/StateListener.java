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
package io.streamthoughts.kafka.connect.filepulse.source;

interface StateListener {

    /**
     * This method is invoked when a file is scheduled by the task.
     * @see FileRecordsPollingConsumer
     *
     * @param context   the file context.
     */
    void onScheduled(final FileInputContext context);

    /**
     * This method is invoked when a file can't be scheduled.
     * @see FileRecordsPollingConsumer
     *
     * @param context   the file context.
     */
    void onInvalid(final FileInputContext context);

    /**
     * This method is invoked when a source file is starting to be read.
     * @see FileRecordsPollingConsumer
     *
     * @param context   the file context.
     */
    void onStart(final FileInputContext context);

    /**
     * This method is invoked when a source file processing is completed.
     * @see FileRecordsPollingConsumer
     *
     * @param context   the file context.
     */
    void onCompleted(final FileInputContext context);

    /**
     * This method is invoked when an error occurred while processing a source file.
     * @see FileRecordsPollingConsumer
     *
     * @param context   the file context.
     */
    void onFailure(final FileInputContext context, final Throwable t);
}
