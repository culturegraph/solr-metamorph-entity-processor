package org.culturegraph.plugin.io;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Scanner;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * A reader that chunks a input stream on a defined record terminator symbol.
 */
public class ChunkReader implements Iterable<String> {

    private Reader reader;
    private String recordTerminator;

    public ChunkReader(Reader reader, String recordTerminator) {
        this.reader = reader;
        this.recordTerminator = recordTerminator;
    }

    public ChunkReader(InputStream inputStream, String recordTerminator) {
        this(inputStream, recordTerminator, StandardCharsets.UTF_8);
    }

    public ChunkReader(InputStream inputStream, String recordTerminator, Charset charset) {
        this.reader = new InputStreamReader(inputStream, charset);
        this.recordTerminator = recordTerminator;
    }

    public Stream<String> records() {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(
                this.iterator(), Spliterator.ORDERED | Spliterator.NONNULL), false);
    }

    @Override
    public Iterator<String> iterator() {
        Scanner sc = new Scanner(reader);
        sc.useDelimiter(recordTerminator);
        return new ScannerIterator(sc);
    }

    public static class ScannerIterator implements Iterator<String> {

        private Scanner scanner;
        private String recordTerminator;
        private String buf;

        public ScannerIterator(Scanner scanner) {
            this.scanner = scanner;
            this.recordTerminator = scanner.delimiter().pattern();
        }

        @Override
        public boolean hasNext() {
            if (scanner.hasNext()) {
                String rec = scanner.next();
                if (!rec.startsWith(recordTerminator) && !rec.trim().isEmpty()) {
                    buf = rec;
                    return true;
                }
            } else {
                buf = null;
            }
            return false;
        }

        @Override
        public String next() {
            if (buf != null) {
                return buf + recordTerminator;
            } else {
                throw new NoSuchElementException();
            }
        }
    }
}
