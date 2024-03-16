/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.fs.iterator;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.fs.reader.text.RowFileInputIterator;
import io.streamthoughts.kafka.connect.filepulse.fs.reader.text.RowFileWithFooterInputIterator;
import io.streamthoughts.kafka.connect.filepulse.reader.FileInputIterator;
import io.streamthoughts.kafka.connect.filepulse.source.FileRecord;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SftpRowFileInputIteratorBuilderTest {

    @Test
    void when_has_no_header_nor_footer_builder_should_add_decorators() {
        SftpRowFileInputIteratorBuilder builder = spy(new SftpRowFileInputIteratorBuilder());

        RowFileInputIterator rowFileInputIterator = mock(RowFileInputIterator.class);
        doReturn(rowFileInputIterator).when(builder).initRowFileInputIterator();

        FileInputIterator<FileRecord<TypedStruct>> result = builder.build();

        verify(builder).initRowFileInputIterator();
        verify(builder, never()).initRowFileWithFooterInputIterator(any());
        verify(builder, never()).initSftpRowFileWithHeadersInputIterator(eq(rowFileInputIterator));
        Assertions.assertThat(result).isEqualTo(rowFileInputIterator);
    }

    @Test
    void when_has_header_builder_should_add_header_decorator() {
        SftpRowFileInputIteratorBuilder builder = spy(new SftpRowFileInputIteratorBuilder());

        RowFileInputIterator rowFileInputIterator = mock(RowFileInputIterator.class);
        SftpRowFileWithHeadersInputIterator rowFileWithHeadersInputIterator =
                mock(SftpRowFileWithHeadersInputIterator.class);
        doReturn(rowFileInputIterator).when(builder).initRowFileInputIterator();

        doReturn(rowFileWithHeadersInputIterator).when(builder)
                .initSftpRowFileWithHeadersInputIterator(eq(rowFileInputIterator));

        builder.withSkipHeaders(1);

        FileInputIterator<FileRecord<TypedStruct>> result = builder.build();

        verify(builder).initRowFileInputIterator();
        verify(builder, never()).initRowFileWithFooterInputIterator(any());
        verify(builder).initSftpRowFileWithHeadersInputIterator(eq(rowFileInputIterator));
        Assertions.assertThat(result).isEqualTo(rowFileWithHeadersInputIterator);
    }

    @Test
    void when_has_footer__builder_should_add_footer_decorator() {
        SftpRowFileInputIteratorBuilder builder = spy(new SftpRowFileInputIteratorBuilder());

        RowFileInputIterator rowFileInputIterator = mock(RowFileInputIterator.class);
        RowFileWithFooterInputIterator rowFileWithFooterInputIterator = mock(RowFileWithFooterInputIterator.class);

        doReturn(rowFileInputIterator).when(builder).initRowFileInputIterator();

        doReturn(rowFileWithFooterInputIterator).when(builder)
                .initRowFileWithFooterInputIterator(eq(rowFileInputIterator));

        builder.withSkipFooters(2);

        FileInputIterator<FileRecord<TypedStruct>> result = builder.build();

        verify(builder).initRowFileInputIterator();
        verify(builder).initRowFileWithFooterInputIterator(eq(rowFileInputIterator));
        verify(builder, never()).initSftpRowFileWithHeadersInputIterator(any());
        Assertions.assertThat(result).isEqualTo(rowFileWithFooterInputIterator);
    }

    @Test
    void when_has_footer_and_header__builder_should_add_decorators() {
        SftpRowFileInputIteratorBuilder builder = spy(new SftpRowFileInputIteratorBuilder());

        RowFileInputIterator rowFileInputIterator = mock(RowFileInputIterator.class);
        RowFileWithFooterInputIterator rowFileWithFooterInputIterator = mock(RowFileWithFooterInputIterator.class);
        SftpRowFileWithHeadersInputIterator rowFileWithHeadersInputIterator =
                mock(SftpRowFileWithHeadersInputIterator.class);
        doReturn(rowFileInputIterator).when(builder).initRowFileInputIterator();

        doReturn(rowFileWithFooterInputIterator).when(builder)
                .initRowFileWithFooterInputIterator(eq(rowFileInputIterator));

        doReturn(rowFileWithHeadersInputIterator).when(builder)
                .initSftpRowFileWithHeadersInputIterator(eq(rowFileWithFooterInputIterator));

        builder.withSkipFooters(2);
        builder.withSkipHeaders(1);

        FileInputIterator<FileRecord<TypedStruct>> result = builder.build();

        verify(builder).initRowFileInputIterator();
        verify(builder).initRowFileWithFooterInputIterator(eq(rowFileInputIterator));
        verify(builder).initSftpRowFileWithHeadersInputIterator(eq(rowFileWithFooterInputIterator));
        Assertions.assertThat(result).isEqualTo(rowFileWithHeadersInputIterator);
    }
}