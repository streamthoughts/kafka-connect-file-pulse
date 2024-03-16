/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.fs;

import io.streamthoughts.kafka.connect.filepulse.source.FileObjectMeta;
import java.io.InputStream;
import java.net.URI;
import java.util.Map;
import org.apache.kafka.common.Configurable;

/**
 * The {@link Storage} class represents the principal interface to access and manipulate a object files.
 */
public interface Storage extends Configurable {

    /**
     * {@inheritDoc}
     */
    @Override
    default void configure(final Map<String, ?> configs) { }

    /**
     *
     * @param uri    the file object {@link URI}.
     * @return       the {@link FileObjectMeta}.
     */
    FileObjectMeta getObjectMetadata(final URI uri);

    /**
     * Checks whether the given object exists and is accessible under this storage.
     *
     * @param uri    the file object {@link URI}.
     * @return       {@code true} if the object exist, otherwise {@code false}.
     */
    boolean exists(final URI uri);

    /**
     * Deletes the object file from this storage.
     *
     * @param uri   the file object {@link URI}.
     * @return      {@code true} if the object has been removed successfully, otherwise {@code false}.
     */
    boolean delete(final URI uri);

    /**
     * Moves the object file from this storage.
     *
     * @param source    the {@link URI} of the source file object.
     * @param dest      the {@link URI} of the file object destination.
     * @return          {@code true} if the object file has been moved successfully, otherwise {@code false}.
     */
    boolean move(final URI source, final URI dest);

    /**
     * Gets a {@link InputStream} for the given object.
     *
     * @param uri    the file object {@link URI}.
     * @return       new {@link {@link InputStream}.
     */
    InputStream getInputStream(final URI uri) throws Exception;
}
