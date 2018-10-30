package org.culturegraph.plugin.io;

import java.util.Iterator;

@Deprecated
public class Marc21Preprocessor implements Iterator<String> {

    private Iterator<String> iterator;

    public Marc21Preprocessor(Iterator<String> iterator) {
        this.iterator = iterator;
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public String next() {
        String s = iterator.next();
        // Remove leading whitespace
        String ltrim = s.replaceAll("^\\s+","");
        // Replace newline and carriage return with a space
        return ltrim.replaceAll("[\\n\\r]", " ");
    }
}
