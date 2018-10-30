package org.culturegraph.plugin.io;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.marc4j.MarcStreamWriter;
import org.marc4j.MarcWriter;
import org.marc4j.MarcXmlReader;
import org.marc4j.marc.Record;

public class MarcConverter implements Iterator<String> {
    private MarcXmlReader reader;
    private ByteArrayOutputStream buffer;
    private MarcWriter writer;
    private Charset encoding;

    public MarcConverter(MarcXmlReader reader) {
        this.reader = reader;
        this.encoding = StandardCharsets.UTF_8;
        this.buffer = new ByteArrayOutputStream();
        this.writer = new MarcStreamWriter(buffer, encoding.name());
    }


    @Override
    public boolean hasNext() {
        return reader.hasNext();
    }

    @Override
    public String next() {
        Record rec = reader.next();
        return convertToRawMarc(rec);
    }

    private String convertToRawMarc(Record record) {
        writer.write(record);

        try {
            buffer.flush();
        } catch (IOException e) {
            throw new NoSuchElementException("Could not flush buffer!");
        }

        String result = new String(buffer.toByteArray(), encoding);
        buffer.reset();
        return result;
    }
}
