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
package io.streamthoughts.kafka.connect.filepulse.fs;

import static java.time.Instant.ofEpochSecond;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.jcraft.jsch.ChannelSftp.LsEntry;
import com.jcraft.jsch.SftpATTRS;
import io.streamthoughts.kafka.connect.filepulse.fs.client.SftpClient;
import io.streamthoughts.kafka.connect.filepulse.fs.filter.RegexFileListFilter;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectMeta;
import io.streamthoughts.kafka.connect.filepulse.source.GenericFileObjectMeta;
import java.net.URI;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class SftpFilesystemListingTest {

    @Test
    @Order(1)
    public void when_no_filter_regex_specified_and_entries_for_files_and_directories_listObjects_should_build_metadata_only_for_files() {
        Stream<LsEntry> entries = Stream.of(Fixture.visitorEntry, Fixture.fullReferrerEntry, Fixture.parentDirEntry);

        SftpFilesystemListing listing = buildSftpFilesystemListingMock(entries, Collections.emptyList());

        Collection<FileObjectMeta> result = listing.listObjects();

        assertThat(result).hasSize(2);
        assertThat(result).containsExactlyInAnyOrder(Fixture.visitorMetadata, Fixture.fullReferrerMetadata);
    }

    @Test
    @Order(2)
    public void when_filter_regex_specified_and_entries_for_files_and_directories_listObjects_should_build_metadata_only_for_files_matching_pattern() {
        Stream<LsEntry> entries = Stream.of(Fixture.visitorEntry, Fixture.fullReferrerEntry, Fixture.parentDirEntry);
        RegexFileListFilter regexFileListFilter = buildRegexFilter(Fixture.visitorRegexPattern);

        SftpFilesystemListing listing = buildSftpFilesystemListingMock(entries, Collections.singletonList(regexFileListFilter));

        Collection<FileObjectMeta> result = listing.listObjects();

        assertThat(result).hasSize(1);
        assertThat(result).containsExactlyInAnyOrder(Fixture.visitorMetadata);
    }

    @SneakyThrows
    private SftpFilesystemListing buildSftpFilesystemListingMock(Stream<LsEntry> entries, List<FileListFilter> filters) {
        SftpFilesystemListingConfig config = mock(SftpFilesystemListingConfig.class);
        when(config.getSftpListingDirectoryPath()).thenReturn(Fixture.path);

        SftpClient client = spy(new SftpClient(config));
        doReturn(entries).when(client).listAll(anyString());

        SftpFilesystemListing listing = spy(new SftpFilesystemListing(filters));

        doReturn(client).when(listing).getSftpClient();
        doReturn(config).when(listing).getConfig();

        return listing;
    }

    private RegexFileListFilter buildRegexFilter(String pattern) {
        RegexFileListFilter filter = new RegexFileListFilter();
        Map<String, String> config = Map.of(Fixture.regexPatternConfig, pattern);
        filter.configure(config);

        return filter;
    }

    private static LsEntry buildEntryMock(String entryName, int entryMTime, long entrySize, boolean isRegularFile) {
        LsEntry entry = mock(LsEntry.class);
        SftpATTRS attrs = mock(SftpATTRS.class);

        lenient().when(attrs.getMTime()).thenReturn(entryMTime);
        lenient().when(attrs.getSize()).thenReturn(entrySize);
        lenient().when(attrs.isReg()).thenReturn(isRegularFile);

        lenient().when(entry.getFilename()).thenReturn(entryName);
        when(entry.getAttrs()).thenReturn(attrs);

        return entry;
    }

    private static URI buildFileURI(String fileName) {
        return URI.create(String.format("%s/%s", Fixture.path, fileName));
    }

    private static int getEpochSeconds(String timestamp) {
        return (int) Instant.parse(timestamp).getEpochSecond();
    }

    interface Fixture {
        String path = "/userdata";
        String regexPatternConfig = "file.filter.regex.pattern";
        String visitorRegexPattern = "^getFullVisitors[a-zA-Z0-9_-]+.csv";

        String visitorFileName = "getFullVisitors_2023-01-19_08.csv";
        String fullReferrerFileName = "getFullReferrer_2022-11-21_2022-12-21.csv";
        String parentDirFileName = ".";

        int visitorMTime = getEpochSeconds("2023-02-16T13:45:00Z");
        int fullReferrerMTime = getEpochSeconds("2023-02-12T06:45:00Z");
        int parentDirMTime = getEpochSeconds("2023-05-10T10:11:00Z");

        long visitorSize = 1024;
        long fullReferrerSize = 2048;
        long parentDirSize = 96;

        LsEntry visitorEntry = buildEntryMock(visitorFileName, visitorMTime, visitorSize, true);
        LsEntry fullReferrerEntry = buildEntryMock(fullReferrerFileName, fullReferrerMTime, fullReferrerSize, true);
        LsEntry parentDirEntry = buildEntryMock(parentDirFileName, parentDirMTime, parentDirSize, false);

        GenericFileObjectMeta visitorMetadata =
                new GenericFileObjectMeta.Builder()
                        .withName(visitorFileName)
                        .withUri(buildFileURI(visitorFileName))
                        .withLastModified(ofEpochSecond(visitorMTime))
                        .withContentLength(visitorSize)
                        .build();

        GenericFileObjectMeta fullReferrerMetadata =
                new GenericFileObjectMeta.Builder()
                        .withName(fullReferrerFileName)
                        .withUri(buildFileURI(fullReferrerFileName))
                        .withLastModified(ofEpochSecond(fullReferrerMTime))
                        .withContentLength(fullReferrerSize)
                        .build();
    }
}
