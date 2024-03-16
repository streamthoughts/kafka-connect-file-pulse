/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.fs.utils;

import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Aliyun oss uri util
 */
public class AliyunOSSURI {
    private static final Pattern ENDPOINT_PATTERN = Pattern.compile("^(.+\\.)?s3[.-]([a-z0-9-]+)\\.");
    private URI uri;
    private boolean isPathStyle;
    private String bucket;
    private String key;


    public AliyunOSSURI(String str) {
        this(str, true);
    }

    public AliyunOSSURI(String str, boolean urlEncode) {
        this(URI.create(preprocessUrlStr(str, urlEncode)), urlEncode);
    }

    public AliyunOSSURI(URI uri) {
        this(uri, false);
    }

    private AliyunOSSURI(URI uri, boolean urlEncode) {
        if (uri == null) {
            throw new IllegalArgumentException("uri cannot be null");
        }
        initialize(uri, urlEncode);
    }

    private static String preprocessUrlStr(String str, boolean encode) {
        if (encode) {
            return URLEncoder.encode(str, StandardCharsets.UTF_8).replace("%3A", ":").replace("%2F", "/")
                    .replace("+", "%20");
        } else {
            return str;
        }
    }

    private static String decode(String str) {
        if (str == null) {
            return null;
        } else {
            for (int i = 0; i < str.length(); ++i) {
                if (str.charAt(i) == '%') {
                    return decode(str, i);
                }
            }

            return str;
        }
    }

    private static String decode(String str, int firstPercent) {
        StringBuilder builder = new StringBuilder();
        builder.append(str, 0, firstPercent);
        appendDecoded(builder, str, firstPercent);
        for (int i = firstPercent + 3; i < str.length(); ++i) {
            if (str.charAt(i) == '%') {
                appendDecoded(builder, str, i);
                i += 2;
            } else {
                builder.append(str.charAt(i));
            }
        }

        return builder.toString();
    }

    private static void appendDecoded(StringBuilder builder, String str, int index) {
        if (index > str.length() - 3) {
            throw new IllegalStateException("Invalid percent-encoded string:\"" + str + "\".");
        } else {
            char first = str.charAt(index + 1);
            char second = str.charAt(index + 2);
            char decoded = (char) (fromHex(first) << 4 | fromHex(second));
            builder.append(decoded);
        }
    }

    private static int fromHex(char c) {
        if (c < '0') {
            throw new IllegalStateException(
                    "Invalid percent-encoded string: bad character '" + c + "' in escape sequence.");
        } else if (c <= '9') {
            return c - 48;
        } else if (c < 'A') {
            throw new IllegalStateException(
                    "Invalid percent-encoded string: bad character '" + c + "' in escape sequence.");
        } else if (c <= 'F') {
            return c - 65 + 10;
        } else if (c < 'a') {
            throw new IllegalStateException(
                    "Invalid percent-encoded string: bad character '" + c + "' in escape sequence.");
        } else if (c <= 'f') {
            return c - 97 + 10;
        } else {
            throw new IllegalStateException(
                    "Invalid percent-encoded string: bad character '" + c + "' in escape sequence.");
        }
    }

    private void initialize(URI uri, boolean urlEncode) {
        this.uri = uri;
        if ("oss".equalsIgnoreCase(uri.getScheme())) {
            this.isPathStyle = false;
            this.bucket = uri.getAuthority();
            if (this.bucket == null) {
                throw new IllegalArgumentException("Invalid OSS URI: no bucket: " + uri);
            }
            String path = uri.getPath();
            this.key = (path.length() <= 1) ? null : uri.getPath().substring(1);
            return;
        }
        parseHost(urlEncode);
    }

    private void parseHost(boolean urlEncode) {
        String path = uri.getHost();
        if (path == null) {
            throw new IllegalArgumentException("Invalid OSS URI: no hostname: " + uri);
        }
        Matcher matcher = ENDPOINT_PATTERN.matcher(path);
        if (!matcher.find()) {
            throw new IllegalArgumentException(
                    "Invalid OSS URI: hostname does not appear to be a valid OSS endpoint: " + uri);
        }
        String prefix = matcher.group(1);
        if (prefix != null && !prefix.isEmpty()) {
            this.isPathStyle = false;
            this.bucket = prefix.substring(0, prefix.length() - 1);
            path = uri.getPath();
            if (path != null && !path.isEmpty() && !"/".equals(uri.getPath())) {
                this.key = uri.getPath().substring(1);
            }
            return;
        }
        this.isPathStyle = true;
        decodeHost(urlEncode);
    }

    private void decodeHost(boolean urlEncode) {
        String path = urlEncode ? uri.getPath() : uri.getRawPath();
        if (!"".equals(path) && !"/".equals(path)) {
            int index = path.indexOf(47, 1);
            if (index == -1) {
                this.bucket = decode(path.substring(1));
                this.key = null;
            } else if (index == path.length() - 1) {
                this.bucket = decode(path.substring(1, index));
                this.key = null;
            } else {
                this.bucket = decode(path.substring(1, index));
                this.key = decode(path.substring(index + 1));
            }
        }
    }

    public URI getURI() {
        return this.uri;
    }

    public boolean isPathStyle() {
        return this.isPathStyle;
    }

    public String getBucket() {
        return this.bucket;
    }

    public String getKey() {
        return this.key;
    }

    @Override
    public String toString() {
        return this.uri.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o != null && this.getClass() == o.getClass()) {
            AliyunOSSURI that = (AliyunOSSURI) o;
            if (this.isPathStyle != that.isPathStyle) {
                return false;
            } else if (!this.uri.equals(that.uri)) {
                return false;
            } else {
                label58:
                {
                    if (this.bucket != null) {
                        if (this.bucket.equals(that.bucket)) {
                            break label58;
                        }
                    } else if (that.bucket == null) {
                        break label58;
                    }

                    return false;
                }

                if (this.key != null) {
                    if (!this.key.equals(that.key)) {
                        return false;
                    }
                } else if (that.key != null) {
                    return false;
                }
                return false;
            }
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        int result = this.uri.hashCode();
        result = 31 * result + (this.isPathStyle ? 1 : 0);
        result = 31 * result + (this.bucket != null ? this.bucket.hashCode() : 0);
        result = 31 * result + (this.key != null ? this.key.hashCode() : 0);
        return result;
    }
}