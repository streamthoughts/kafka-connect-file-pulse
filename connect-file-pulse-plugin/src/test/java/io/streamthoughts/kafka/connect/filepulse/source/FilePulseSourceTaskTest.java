/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.source;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class FilePulseSourceTaskTest {


    @Test
    public void test_MaxConsecutiveAttempts() {
        final FilePulseSourceTask.MaxConsecutiveAttempts attempts = new FilePulseSourceTask.MaxConsecutiveAttempts(3);
        assertEquals(3, attempts.getRemaining());
        assertTrue(attempts.checkAndDecrement());
        assertTrue(attempts.checkAndDecrement());
        assertTrue(attempts.checkAndDecrement());
        assertEquals(0, attempts.getRemaining());
    }

}