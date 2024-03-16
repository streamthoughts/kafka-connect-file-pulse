/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.expression.function.strings;

import static io.streamthoughts.kafka.connect.filepulse.internal.Silent.unchecked;

import io.streamthoughts.kafka.connect.filepulse.data.TypedValue;
import io.streamthoughts.kafka.connect.filepulse.expression.function.AbstractTransformExpressionFunction;
import java.math.BigInteger;
import java.security.MessageDigest;

public class Md5 extends AbstractTransformExpressionFunction {

  private static final MessageDigest DIGEST = unchecked(() -> MessageDigest.getInstance("MD5"));

  /**
   * {@inheritDoc}
   */
  @Override
  public TypedValue transform(final TypedValue value) {
    byte[] digest = DIGEST.digest(value.getBytes());
    return TypedValue.string(new BigInteger(1, digest).toString(16));
  }
}
