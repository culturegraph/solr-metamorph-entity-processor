package org.culturegraph.plugin.io;

import java.io.*;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class DecompressedInputStreamTest
{

    @Test
    public void decompressUncompressedInputStream() throws IOException
    {
        ByteArrayInputStream inputStream = new ByteArrayInputStream("1\n2\n".getBytes());

        InputStream decompressedInputStream = DecompressedInputStream.of(inputStream);
        try (BufferedReader br = new BufferedReader(new InputStreamReader(decompressedInputStream)))
        {
            String result = br.lines().collect(Collectors.joining(" "));
            assertEquals("1 2", result);
        }
    }

    @Test
    public void decompressCompressedInputStream() throws IOException
    {
        ByteArrayOutputStream gzipBuffer = new ByteArrayOutputStream();
        OutputStream outputStream = new GZIPOutputStream(gzipBuffer);
        try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(outputStream)))
        {
            bw.write("1\n2\n");
        }

        GZIPInputStream inputStream = new GZIPInputStream(new ByteArrayInputStream(gzipBuffer.toByteArray()));

        InputStream decompressedInputStream = DecompressedInputStream.of(inputStream);
        try (BufferedReader br = new BufferedReader(new InputStreamReader(decompressedInputStream)))
        {
            String result = br.lines().collect(Collectors.joining(" "));
            assertEquals("1 2", result);
        }
    }
}