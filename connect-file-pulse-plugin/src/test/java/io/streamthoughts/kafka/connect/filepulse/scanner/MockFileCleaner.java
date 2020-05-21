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
package io.streamthoughts.kafka.connect.filepulse.scanner;

import io.streamthoughts.kafka.connect.filepulse.clean.FileCleanupPolicy;
import io.streamthoughts.kafka.connect.filepulse.source.SourceFile;

import java.io.File;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class MockFileCleaner implements FileCleanupPolicy {

    private final List<File> succeed = new LinkedList<>();
    private final List<File> failed = new LinkedList<>();

    private final boolean cleanUpReturn;

    MockFileCleaner(boolean cleanUpReturn) {
        this.cleanUpReturn = cleanUpReturn;
    }

    List<File> getSucceed() {
        return succeed;
    }

    List<File> getFailed() {
        return failed;
    }

    @Override
    public void configure(final Map<String, ?> configs) {

    }

    @Override
    public boolean onSuccess(SourceFile source) {
        this.succeed.add(source.file());
        return cleanUpReturn;
    }

    @Override
    public boolean onFailure(SourceFile source) {
        this.failed.add(source.file());
        return cleanUpReturn;
    }
}