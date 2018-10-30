package org.culturegraph.plugin.io;

import java.io.ByteArrayInputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class ChunkReaderTest {

    private static String recordTerminator = "\u001D";

    @Test
    public void iterateOverSingleRecord() {
        Charset utf8 = StandardCharsets.UTF_8;
        String record = "hello world" + recordTerminator;
        ByteArrayInputStream inputStream = new ByteArrayInputStream(record.getBytes(utf8));
        ChunkReader reader = new ChunkReader(inputStream, recordTerminator);
        Iterator<String> iter = reader.iterator();

        assertThat(iter.hasNext(), is(true));
        assertThat(iter.next(), equalTo(record));
        assertThat(iter.hasNext(), is(false));
    }

    @Test
    public void streamSingleRecord() {
        Charset utf8 = StandardCharsets.UTF_8;
        String record = "hello world" + recordTerminator;
        ByteArrayInputStream inputStream = new ByteArrayInputStream(record.getBytes(utf8));
        ChunkReader reader = new ChunkReader(inputStream, recordTerminator);

        String result = reader.records().findFirst().orElse("");

        assertThat(result.isEmpty(), is(false));
        assertThat(result, equalTo(record));
    }

    @Test
    public void iterateOverTwoRecords() {
        Charset utf8 = StandardCharsets.UTF_8;

        String record1 = "hello world" + recordTerminator;
        String record2 = "good bye" + recordTerminator;
        byte[] bytes = (record1 + record2).getBytes(utf8);
        ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
        ChunkReader reader = new ChunkReader(inputStream, recordTerminator);
        Iterator<String> iter = reader.iterator();

        assertThat(iter.hasNext(), is(true));
        assertThat(iter.next(), equalTo(record1));
        assertThat(iter.hasNext(), is(true));
        assertThat(iter.next(), equalTo(record2));
        assertThat(iter.hasNext(), is(false));
    }

    @Test
    public void streamTwoRecords() {
        Charset utf8 = StandardCharsets.UTF_8;

        String record1 = "hello world" + recordTerminator;
        String record2 = "good bye" + recordTerminator;
        byte[] bytes = (record1 + record2).getBytes(utf8);
        ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
        ChunkReader reader = new ChunkReader(inputStream, recordTerminator);

        List<String> resultList = reader.records().collect(Collectors.toList());

        assertThat(resultList.size(), is(equalTo(2)));
        assertThat(resultList.get(0), is(equalTo(record1)));
        assertThat(resultList.get(1), is(equalTo(record2)));
    }

    @Test
    public void dropEmptyRecordWhenIterating() {
        Charset utf8 = StandardCharsets.UTF_8;

        String record1 = "hello world" + recordTerminator;
        String record2 = "" + recordTerminator;
        byte[] bytes = (record1 + record2).getBytes(utf8);
        ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
        ChunkReader reader = new ChunkReader(inputStream, recordTerminator);
        Iterator<String> iter = reader.iterator();

        assertThat(iter.hasNext(), is(true));
        assertThat(iter.next(), equalTo(record1));
        assertThat(iter.hasNext(), is(false));
    }

    @Test
    public void dropEmptyRecordWhenStreaming() {
        Charset utf8 = StandardCharsets.UTF_8;

        String record1 = "hello world" + recordTerminator;
        String record2 = "" + recordTerminator;
        byte[] bytes = (record1 + record2).getBytes(utf8);
        ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
        ChunkReader reader = new ChunkReader(inputStream, recordTerminator);

        List<String> resultList = reader.records().collect(Collectors.toList());

        assertThat(resultList.size(), is(equalTo(1)));
        assertThat(resultList.get(0), is(equalTo(record1)));
    }

    @Test(expected = NoSuchElementException.class)
    public void readAfterEndRecord() {
        Charset utf8 = StandardCharsets.UTF_8;
        String record = "hello world" + recordTerminator;
        ByteArrayInputStream inputStream = new ByteArrayInputStream(record.getBytes(utf8));
        ChunkReader reader = new ChunkReader(inputStream, recordTerminator);
        Iterator<String> iter = reader.iterator();

        iter.next();
        iter.next();
    }
}