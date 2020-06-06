package com.alibaba.tailbase;

import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class UtilsTest {
    @Test
    public void readFile() throws Exception {
        long start = System.currentTimeMillis();
        String path = "trace1.data";
        InputStream inputStream = Files.newInputStream(Paths.get(path));
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
        String line;
        long count = 0;
        while ((line = bufferedReader.readLine()) != null) {
            count++;
        }
        long end = System.currentTimeMillis();
        System.out.println("Cost: " + (end - start) + " | " + "Lines: " + count);
    }
}
