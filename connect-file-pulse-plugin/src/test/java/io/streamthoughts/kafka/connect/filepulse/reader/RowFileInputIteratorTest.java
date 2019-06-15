package io.streamthoughts.kafka.connect.filepulse.reader;

import io.streamthoughts.kafka.connect.filepulse.offset.SourceMetadata;
import io.streamthoughts.kafka.connect.filepulse.offset.SourceOffset;
import io.streamthoughts.kafka.connect.filepulse.source.FileInputContext;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;

import static org.junit.Assert.*;

public class RowFileInputIteratorTest {

    private static final String LF = "\n";

    private static final int NLINES = 10;

    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();

    private File file;
    private BufferedWriter writer;

    private FileInputContext context;


    @Before
    public void setUp() throws IOException {
        file = testFolder.newFile();
        writer = Files.newBufferedWriter(file.toPath(), Charset.defaultCharset());
        generateLines(writer);
        context = new FileInputContext(SourceMetadata.fromFile(file));

    }

    @Test
    public void test() {
        RowFileInputIterator iterator = RowFileInputIterator.newBuilder()
                .withContext(context)
                .withIteratorManager(new IteratorManager())
                .build();
        iterator.seekTo(new SourceOffset(0, 0, System.currentTimeMillis()));
        while(iterator.hasNext()) {
            RecordsIterable<FileInputRecord> next = iterator.next();
            assertNotNull(next);

        }

        FileInputContext context = iterator.context();
        assertEquals(file.length(), context.offset().position());
        assertEquals(NLINES, context.offset().rows());
    }

    private void generateLines(final BufferedWriter writer) throws IOException {

        for (int i = 0; i < NLINES; i++) {
            String line = "00000000-" + i;
            writer.write(line);
            if (i + 1 < NLINES) {
                writer.write(LF);
            }
        }
        writer.flush();
    }
}