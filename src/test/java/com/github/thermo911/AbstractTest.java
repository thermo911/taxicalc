package com.github.thermo911;

import java.io.IOException;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;

import com.github.thermo911.data.Trip;
import org.junit.jupiter.api.io.TempDir;

public class AbstractTest {

    protected static final LocalDateTime TIME_START_POINT = LocalDateTime.ofEpochSecond(0, 0, ZoneOffset.UTC);

    @TempDir
    protected static Path tempDir;

    protected static String makeParquetFile(List<Trip> trips) throws IOException {
        return makeParquetFile(trips, "trips.parquet");
    }

    protected static String makeParquetFile(List<Trip> trips, String filename) throws IOException {
        Path file = tempDir.resolve(filename);
        ParquetUtils.writeTripsToParquet(trips, file.toString());
        return file.toString();
    }
}
