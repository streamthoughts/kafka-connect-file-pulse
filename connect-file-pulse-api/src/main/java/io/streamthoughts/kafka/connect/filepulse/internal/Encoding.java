/*
 * Copyright 2022 StreamThoughts.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.streamthoughts.kafka.connect.filepulse.internal;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Utility class for encoding and decoding bytes array.
 */
public enum Encoding {

    HEX (Encoding::bytesToHex),
    UTF8(value -> new String(value, StandardCharsets.UTF_8)),
    ASCII(value -> new String(value, StandardCharsets.US_ASCII)),
    BASE64(value -> Base64.getEncoder().encodeToString(value));

    private final Function<byte[], String> encoder;

    /**
     * Creates a new {@link Encoding} instance.
     *
     * @param encoder   the encoding function.
     */
    Encoding(final Function<byte[], String> encoder) {
        this.encoder = Objects.requireNonNull(encoder, "'encode' should not be null");
    }

    public static Encoding from(final String value) {

        for (Encoding encoding : Encoding.values()) {
            if (value.equalsIgnoreCase(encoding.name())) {
                return encoding;
            }
        }

        String supported = Arrays.stream(Encoding.values())
                .map(Enum::name)
                .map(String::toLowerCase)
                .collect(Collectors.joining(", ", "[ ", " ]"));

        throw new IllegalArgumentException("Unknown encoding type '" + value + "'. "
                + "Supported encoding types are: " + supported +".");
    }

    public String encode(final byte[] bytes) {
        return encoder.apply(bytes);
    }

    private static final byte[] HEX_ARRAY = "0123456789ABCDEF".getBytes(StandardCharsets.US_ASCII);

    /* https://stackoverflow.com/questions/9655181/how-to-convert-a-byte-array-to-a-hex-string-in-java */
    private static String bytesToHex(byte[] bytes) {
        byte[] hexChars = new byte[bytes.length * 2];
        for (int j = 0; j < bytes.length; j++) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = HEX_ARRAY[v >>> 4];
            hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
        }
        return new String(hexChars, StandardCharsets.UTF_8);
    }
}
