/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.fs;

import static java.time.Instant.ofEpochMilli;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import io.streamthoughts.kafka.connect.filepulse.fs.client.SmbClient;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectMeta;
import io.streamthoughts.kafka.connect.filepulse.source.GenericFileObjectMeta;
import java.net.URI;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SmbFileSystemListingTest {

    @Test
    @Order(1)
    void when_no_filter_specified_listObjects_should_return_all_files_metadata() {
        Stream<FileObjectMeta> entries = Stream.of(Fixture.VISITOR_META, Fixture.REFERRER_META);

        SmbFileSystemListing listing = buildSmbFilesystemListingMock(entries, Collections.emptyList());

        Collection<FileObjectMeta> result = listing.listObjects();

        assertThat(result).hasSize(2);
        assertThat(result).containsExactlyInAnyOrder(Fixture.VISITOR_META, Fixture.REFERRER_META);
    }

    @Test
    @Order(2)
    void when_filter_specified_listObjects_should_return_only_matching_files_metadata() {
        Stream<FileObjectMeta> entries = Stream.of(Fixture.VISITOR_META, Fixture.REFERRER_META);

        SmbFileSystemListing listing = buildSmbFilesystemListingMock(entries, Collections.singletonList(files ->
                files.stream()
                        .filter(f -> f.name().matches(Fixture.VISITOR_REGEX))
                        .collect(Collectors.toList())));

        Collection<FileObjectMeta> result = listing.listObjects();

        assertThat(result).hasSize(1);
        assertThat(result).containsExactlyInAnyOrder(Fixture.VISITOR_META);
    }

    @SneakyThrows
    private SmbFileSystemListing buildSmbFilesystemListingMock(Stream<FileObjectMeta> entries,
                                                               List<FileListFilter> filters) {
        SmbFileSystemListingConfig config = mock(SmbFileSystemListingConfig.class);
        when(config.getSmbDirectoryPath()).thenReturn(Fixture.PATH);
        when(config.getSmbHost()).thenReturn("smb-host");
        when(config.getSmbShare()).thenReturn("share");

        SmbClient client = mock(SmbClient.class);
        doReturn(entries).when(client).listFiles(anyString());

        SmbFileSystemListing listing = spy(new SmbFileSystemListing(filters));
        doReturn(client).when(listing).getSmbClient();
        doReturn(config).when(listing).getConfig();

        return listing;
    }

    private static URI buildFileURI(String fileName) {
        return URI.create(String.format("%s/%s", Fixture.PATH, fileName));
    }

    interface Fixture {
        String PATH = "/userdata";
        String VISITOR_REGEX = "^visitors_[a-zA-Z0-9_-]+.csv";

        String VISITOR_NAME = "visitors_2025-01-01.csv";
        String REFERRER_NAME = "referrer_2025-01-01.csv";

        long VISITOR_MTIME = Instant.parse("2025-01-01T00:00:00Z").toEpochMilli();
        long REFERRER_MTIME = Instant.parse("2025-01-02T00:00:00Z").toEpochMilli();

        long VISITOR_SIZE = 1024L;
        long REFERRER_SIZE = 2048L;

        FileObjectMeta VISITOR_META = new GenericFileObjectMeta.Builder()
                .withName(VISITOR_NAME)
                .withUri(buildFileURI(VISITOR_NAME))
                .withLastModified(ofEpochMilli(VISITOR_MTIME))
                .withContentLength(VISITOR_SIZE)
                .build();

        FileObjectMeta REFERRER_META = new GenericFileObjectMeta.Builder()
                .withName(REFERRER_NAME)
                .withUri(buildFileURI(REFERRER_NAME))
                .withLastModified(ofEpochMilli(REFERRER_MTIME))
                .withContentLength(REFERRER_SIZE)
                .build();
    }
}
