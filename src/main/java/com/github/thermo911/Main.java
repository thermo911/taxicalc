package com.github.thermo911;

import java.io.IOException;
import java.nio.file.Path;
import java.time.LocalDateTime;

public class Main {

    private static final String PATH = "src/main/resources/data";

    public static void main(String[] args) throws IOException {
        LocalDateTime start = LocalDateTime.of(2019, 12, 31, 15, 33, 50);
        LocalDateTime end = LocalDateTime.of(2019, 12, 31, 15, 34, 7);
        try (AverageDistances averageDistances = new AverageDistancesImpl()) {
            averageDistances.init(Path.of(PATH));
            System.out.println("Initialized");
            System.out.println(averageDistances.getAverageDistances(start, end));
        }
    }
}