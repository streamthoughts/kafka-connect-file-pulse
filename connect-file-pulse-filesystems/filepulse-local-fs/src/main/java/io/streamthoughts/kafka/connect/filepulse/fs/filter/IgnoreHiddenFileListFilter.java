/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.fs.filter;

import io.streamthoughts.kafka.connect.filepulse.fs.PredicateFileListFilter;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectMeta;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class IgnoreHiddenFileListFilter extends PredicateFileListFilter {

  private static final Logger LOG = LoggerFactory.getLogger(IgnoreHiddenFileListFilter.class);

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean test(final FileObjectMeta file) {
    if (file.uri().getScheme().equals("file")) {
      try {
        return !Files.isHidden(Paths.get(file.uri()));
      } catch (IOException e) {
        LOG.warn("{}", e.getMessage());
      }
    }
    return true;
  }
}
