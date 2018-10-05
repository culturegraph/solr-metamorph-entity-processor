package org.culturegraph.plugin.io;

import java.io.*;
import java.util.zip.GZIPInputStream;

public class DecompressedInputStream {
    public static InputStream of(InputStream inputStream) throws IOException {
        PushbackInputStream pushbackInputStream = new PushbackInputStream(inputStream, 2);
        byte[] signature = new byte[2];
        int length = pushbackInputStream.read(signature);
        pushbackInputStream.unread(signature, 0, length);

        boolean isGzipped = ((signature[0] == (byte) (GZIPInputStream.GZIP_MAGIC)) && (signature[1] == (byte) (GZIPInputStream.GZIP_MAGIC >> 8)));
        if (isGzipped) {
            final int kb64 = 65536;
            return new GZIPInputStream(pushbackInputStream, kb64);
        } else {
            return pushbackInputStream;
        }
    }

    public static InputStream of(File f) throws IOException {
        return of(new FileInputStream(f));
    }
}
