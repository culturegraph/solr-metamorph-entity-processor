package com.github.eberhardtj;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItems;

public class MiscTest {

    @Test
    public void stringWithCommaToList() {
        String s = "/tmp/file1.txt,/tmp/file2.txt";

        List<String> list = Arrays.stream(s.split(",")).collect(Collectors.toList());

        assertThat(list, hasItems("/tmp/file1.txt", "/tmp/file2.txt"));
    }
}
