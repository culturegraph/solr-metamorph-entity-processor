package org.culturegraph.plugin;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

import org.culturegraph.plugin.io.MarcConverter;
import org.junit.Test;
import org.marc4j.MarcStreamReader;
import org.marc4j.MarcXmlReader;
import org.marc4j.MarcXmlWriter;
import org.xml.sax.InputSource;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

public class MarcConverterTest {

    private int leaderLength = 24;
    private String marcRecordTerminator = "\u001D";

    @Test
    public void readSingleMarcXmlRecordTest() throws Exception {
        String collection = "src/test-files/record.xml";
        MarcXmlReader reader = new MarcXmlReader(new InputSource(new FileInputStream(collection)));
        Iterator<String> iterator = new MarcConverter(reader);

        while (iterator.hasNext()) {
            String marc = iterator.next();
            assertThat(marc.isEmpty(), is(false));
            assertThat(marc.length(), is(greaterThan(leaderLength)));
            assertThat(marc.endsWith(marcRecordTerminator), is(true));
        }
    }

    @Test
    public void readMultipleMarcXmlRecords() throws Exception {
        String collection = "src/test-files/record-collection.xml";
        MarcXmlReader reader = new MarcXmlReader(new InputSource(new FileInputStream(collection)));
        Iterator<String> iterator = new MarcConverter(reader);

        while (iterator.hasNext()) {
            String marc = iterator.next();
            assertThat(marc.isEmpty(), is(false));
            assertThat(marc.length(), is(greaterThan(leaderLength)));
            assertThat(marc.endsWith(marcRecordTerminator), is(true));
        }
    }
}