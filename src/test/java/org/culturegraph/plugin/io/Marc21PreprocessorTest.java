package org.culturegraph.plugin.io;

import org.junit.Test;

import java.util.Iterator;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

public class Marc21PreprocessorTest {

    @Test
    public void shouldRemoveLeadingWhitespace() {
        String record = "  record";
        Iterator<String> iter = Stream.of(record).iterator();
        Marc21Preprocessor preprocessor = new Marc21Preprocessor(iter);

        String processedRecord = preprocessor.next();
        assertThat(processedRecord, is(equalTo("record")));
    }

    @Test
    public void shouldReplaceCarriageReturnOrNewlineWithSpace() {
        String record = "record\r\nrecord";
        Iterator<String> iter = Stream.of(record).iterator();
        Marc21Preprocessor preprocessor = new Marc21Preprocessor(iter);

        String processedRecord = preprocessor.next();
        assertThat(processedRecord, is(equalTo("record  record")));
    }
}