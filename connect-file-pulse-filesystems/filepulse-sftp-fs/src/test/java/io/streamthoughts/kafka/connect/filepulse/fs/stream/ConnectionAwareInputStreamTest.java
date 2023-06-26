/*
 * Copyright 2023 StreamThoughts.
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
package io.streamthoughts.kafka.connect.filepulse.fs.stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import io.streamthoughts.kafka.connect.filepulse.fs.client.SftpConnection;
import java.io.InputStream;
import java.util.stream.Stream;
import java.util.zip.ZipInputStream;
import lombok.SneakyThrows;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ConnectionAwareInputStreamTest {
    @FunctionalInterface
    interface CheckedFunction<T> {
        void apply(T t) throws Exception;
    }

    private ConnectionAwareInputStream mockConnectionAwareInputStream() {
        SftpConnection connection = mock(SftpConnection.class);
        InputStream delegate = mock(InputStream.class);

        return new ConnectionAwareInputStream(connection, "absolutePath", delegate);
    }

    static Stream<Arguments> when_wrapped_method_is_called_ConnectionAwareInputStream_should_delegate_to_wrapped_stream() {
        return Stream.of(
                arguments((CheckedFunction<ConnectionAwareInputStream>) wrapper -> {
                    wrapper.read();
                    verify(wrapper.delegate).read();
                }),
                arguments((CheckedFunction<ConnectionAwareInputStream>) wrapper -> {
                    wrapper.read(new byte[]{});
                    verify(wrapper.delegate).read(any());
                }),
                arguments((CheckedFunction<ConnectionAwareInputStream>) wrapper -> {
                    wrapper.read(new byte[]{}, 0, 1024);
                    verify(wrapper.delegate).read(any(), eq(0), eq(1024));
                }),
                arguments((CheckedFunction<ConnectionAwareInputStream>) wrapper -> {
                    wrapper.skip(1024);
                    verify(wrapper.delegate).skip(eq(1024L));
                }),
                arguments((CheckedFunction<ConnectionAwareInputStream>) wrapper -> {
                    wrapper.available();
                    verify(wrapper.delegate).available();
                }),
                arguments((CheckedFunction<ConnectionAwareInputStream>) wrapper -> {
                    wrapper.mark(1024);
                    verify(wrapper.delegate).mark(eq(1024));
                }),
                arguments((CheckedFunction<ConnectionAwareInputStream>) wrapper -> {
                    wrapper.reset();
                    verify(wrapper.delegate).reset();
                }),
                arguments((CheckedFunction<ConnectionAwareInputStream>) wrapper -> {
                    wrapper.markSupported();
                    verify(wrapper.delegate).markSupported();
                }),
                arguments((CheckedFunction<ConnectionAwareInputStream>) wrapper -> {
                    wrapper.close();
                    verify(wrapper.delegate).close();
                    verify(wrapper.connection).close();
                })
        );
    }

    @SneakyThrows
    @ParameterizedTest
    @MethodSource
    void when_wrapped_method_is_called_ConnectionAwareInputStream_should_delegate_to_wrapped_stream(
            CheckedFunction<ConnectionAwareInputStream> validator) {

        ConnectionAwareInputStream wrapper = mockConnectionAwareInputStream();
        validator.apply(wrapper);
    }

    private static InputStream getLocalResourceAsStream(String filename) {
        return ClassLoader.getSystemClassLoader().getResourceAsStream(filename);
    }

    @SneakyThrows
    public static Stream<Arguments> when_content_passed_isContentZipped_should_determine_if_zipped_or_not() {
        return Stream.of(
                arguments(false, "data/test_data.csv", InputStream.class),
                arguments(true, "data/test_data.csv.zip", ZipInputStream.class)
        );
    }

    @ParameterizedTest
    @MethodSource
    @SneakyThrows
    void when_content_passed_isContentZipped_should_determine_if_zipped_or_not(Boolean expected, String absolutePath, Class<?> delegateClass) {
        SftpConnection connection = mock(SftpConnection.class);
        InputStream delegate = getLocalResourceAsStream(absolutePath);
        ConnectionAwareInputStream wrapper = new ConnectionAwareInputStream(connection, absolutePath, delegate);
        assertThat(wrapper.isContentZipped()).isEqualTo(expected);
        assertThat(wrapper.delegate).isInstanceOf(delegateClass);
    }
}