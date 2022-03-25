package io.streamthoughts.kafka.connect.filepulse.fs.filter;

import io.streamthoughts.kafka.connect.filepulse.source.FileObjectMeta;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.net.URI;
import java.util.Map;

import static org.junit.Assert.*;

public class SizeFileListFilterTest {

    @Test
    public void should_not_filter_file_given_default_limit_size_bytes() {
        SizeFileListFilter filter = new SizeFileListFilter();
        filter.configure(Map.of());
        Assert.assertTrue(filter.test(newFileObjectMeta(0)));
    }

    @Test
    public void should_filter_file_given_minimum_size_bytes_superior_to_zero() {
        SizeFileListFilter filter = new SizeFileListFilter();
        filter.configure(Map.of(
                SizeFileListFilter.FILE_MINIMUM_SIZE_MS_CONFIG, 1
        ));
        Assert.assertFalse(filter.test(newFileObjectMeta(0)));
    }

    @Test
    public void should_filter_file_given_maximum_size_bytes() {
        SizeFileListFilter filter = new SizeFileListFilter();
        filter.configure(Map.of(
                SizeFileListFilter.FILE_MAXIMUM_SIZE_MS_CONFIG, 0
        ));
        Assert.assertFalse(filter.test(newFileObjectMeta(1)));
    }

    @NotNull
    private FileObjectMeta newFileObjectMeta(final long contentLength) {
        return new FileObjectMeta() {
            @Override
            public URI uri() {
                return null;
            }

            @Override
            public String name() {
                return null;
            }

            @Override
            public Long contentLength() {
                return contentLength;
            }

            @Override
            public Long lastModified() {
                return null;
            }

            @Override
            public ContentDigest contentDigest() {
                return null;
            }

            @Override
            public Map<String, Object> userDefinedMetadata() {
                return null;
            }
        };
    }

}