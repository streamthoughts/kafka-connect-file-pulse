/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
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
