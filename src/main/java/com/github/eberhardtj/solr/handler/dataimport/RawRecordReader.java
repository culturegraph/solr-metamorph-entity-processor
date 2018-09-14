package com.github.eberhardtj.solr.handler.dataimport;

import java.io.Reader;
import java.util.Scanner;

/**
 * A record reader that reads ISO 2709 formatted records as a raw strings.
 *
 * Residing new line or carriage returns will be replaced with a space.
 */
public class RawRecordReader implements RecordReader {

    private static String GROUP_SEPARATOR = "\u001D";

    private Scanner scanner;


    public RawRecordReader(Reader reader) {
        this.scanner = new Scanner(reader);
        this.scanner.useDelimiter(GROUP_SEPARATOR);
    }

    @Override
    public boolean hasNext() {
        return scanner.hasNext();
    }

    @Override
    public String next() {
        String record = scanner.next();
        // remove leading spaces
        record = record.replaceAll("^\\s+", "");
        // replace newline
        record = record.replaceAll("\\n", " ");
        // replace carriage return
        record = record.replaceAll("\\r", " ");
        return record.isEmpty() ? null : record + GROUP_SEPARATOR;
    }
}
