package org.culturegraph.plugin.io;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.NoSuchElementException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class ChunkRecordReaderTest {

    private static String recordTerminator = "\u001D";

    @Test
    public void readSingleRecord() {
        Charset utf8 = StandardCharsets.UTF_8;
        String record = "hello world" + recordTerminator;
        ByteArrayInputStream inputStream = new ByteArrayInputStream(record.getBytes(utf8));
        Iterator<String> iter = new ChunkRecordReader(inputStream, recordTerminator);

        assertThat(iter.hasNext(), is(true));
        assertThat(iter.next(), equalTo(record));
        assertThat(iter.hasNext(), is(false));
    }

    @Test
    public void readTwoRecords() {
        Charset utf8 = StandardCharsets.UTF_8;

        String record1 = "hello world" + recordTerminator;
        String record2 = "good bye" + recordTerminator;
        byte[] bytes = (record1 + record2).getBytes(utf8);
        ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
        Iterator<String> iter = new ChunkRecordReader(inputStream, recordTerminator);

        assertThat(iter.hasNext(), is(true));
        assertThat(iter.next(), equalTo(record1));
        assertThat(iter.hasNext(), is(true));
        assertThat(iter.next(), equalTo(record2));
        assertThat(iter.hasNext(), is(false));
    }

    @Test
    public void readRecordWithTrailingTerminator() {
        Charset utf8 = StandardCharsets.UTF_8;

        String record1 = "hello world" + recordTerminator;
        String record2 = recordTerminator;
        byte[] bytes = (record1 + record2).getBytes(utf8);
        ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
        Iterator<String> iter = new ChunkRecordReader(inputStream, recordTerminator);

        assertThat(iter.hasNext(), is(true));
        assertThat(iter.next(), equalTo(record1));
        assertThat(iter.hasNext(), is(false));
    }

    @Test(expected = NoSuchElementException.class)
    public void readAfterEndRecord() {
        Charset utf8 = StandardCharsets.UTF_8;
        String record = "hello world" + recordTerminator;
        ByteArrayInputStream inputStream = new ByteArrayInputStream(record.getBytes(utf8));
        Iterator<String> iter = new ChunkRecordReader(inputStream, recordTerminator);

        iter.next();
        iter.next();
    }
}