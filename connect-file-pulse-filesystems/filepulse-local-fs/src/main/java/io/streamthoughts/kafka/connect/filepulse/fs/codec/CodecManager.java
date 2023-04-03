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
package io.streamthoughts.kafka.connect.filepulse.fs.codec;

import java.io.File;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple class to manager {@link CodecHandler} handler.
 */
public class CodecManager {

    private static final Logger LOG = LoggerFactory.getLogger(CodecManager.class);

    private List<CodecHandler> codecs = new LinkedList<>();

    /**
     * Creates a new {@link CodecManager} instance.
     */
    public CodecManager() {
        register(new ZipCodec());
        register(new GZipCodec());
        register(new TarballCodec());
    }

    private void register(final CodecHandler codec) {
        LOG.info("Added codec '{}'", codec.getClass());
        codecs.add(codec);
    }

    public CodecHandler getCodecIfCompressedOrNull(final File file) {
        Optional<CodecHandler> optional = codecs.stream()
                .filter(c -> c.canRead(file))
                .findFirst();
        return optional.orElse(null);
    }
}
