package org.culturegraph.plugin.io;

import java.io.InputStream;
import java.io.Reader;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Scanner;

/**
 * A reader that chunks a input stream on a defined record terminator symbol.
 */
@Deprecated
public class ChunkRecordReader implements Iterator<String> {
    private Scanner scanner;
    private String recordTerminator;
    private String buffer;

    public ChunkRecordReader(InputStream inputStream, String recordTerminator) {
        this(inputStream, recordTerminator, "UTF-8");
    }

    public ChunkRecordReader(InputStream inputStream, String recordTerminator, String charsetName) {
        this.scanner = new Scanner(inputStream, charsetName);
        this.scanner.useDelimiter(recordTerminator);
        this.recordTerminator = recordTerminator;
    }

    public ChunkRecordReader(Reader reader, String recordTerminator) {
        this.scanner = new Scanner(reader);
        this.scanner.useDelimiter(recordTerminator);
        this.recordTerminator = recordTerminator;
    }

    @Override
    public boolean hasNext() {
        if (hasBuffer()) {
            return true;
        }

        if (scanner.hasNext()) {
            String chunk = scanner.next();
            if (!chunk.startsWith(recordTerminator) && !chunk.trim().isEmpty()) {
                buffer(chunk);
                return true;
            }
        } else {
            resetBuffer();
        }
        return false;
    }

    @Override
    public String next() {
        if (hasBuffer()) {
            String result = buffer + recordTerminator;
            resetBuffer();
            return result;
        } else {
            throw new NoSuchElementException();
        }
    }

    private void buffer(String s) {
        buffer = s;
    }

    private void resetBuffer() {
        buffer = null;
    }

    private boolean hasBuffer() {
        return buffer != null;
    }
}
