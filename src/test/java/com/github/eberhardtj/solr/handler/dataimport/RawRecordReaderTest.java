package com.github.eberhardtj.solr.handler.dataimport;

import org.junit.Test;

import java.io.Reader;
import java.io.StringReader;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class RawRecordReaderTest {

    private RecordReader recordReader;

    @Test
    public void readLines() throws Exception {
        String record1 = "record1\u001D";
        String record2 = "record2\u001D";

        Reader reader = new StringReader(record1 + record2);
        RecordReader recordReader = new RawRecordReader(reader);

        assertThat(recordReader.hasNext(), equalTo(true));
        assertThat(recordReader.next(), equalTo(record1));
        assertThat(recordReader.hasNext(), equalTo(true));
        assertThat(recordReader.next(), equalTo(record2));
        assertThat(recordReader.hasNext(), equalTo(false));
    }

    @Test
    public void replaceNewline() {
        String record = "record1\nabstract\u001D";
        String expectedRecord = record.replaceAll("\\n", " ");

        Reader reader = new StringReader(record);
        RecordReader recordReader = new RawRecordReader(reader);

        assertThat(recordReader.hasNext(), equalTo(true));
        assertThat(recordReader.next(), equalTo(expectedRecord));
        assertThat(recordReader.hasNext(), equalTo(false));
    }
}