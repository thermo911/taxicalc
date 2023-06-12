package com.github.thermo911;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import com.github.thermo911.conf.Config;
import com.github.thermo911.data.Trip;
import com.github.thermo911.data.TripDataset;
import com.github.thermo911.parquet.ParquetTripDataset;
import com.github.thermo911.stat.TripAccumulator;

public class AverageDistancesImpl implements AverageDistances {

    private List<? extends TripDataset> tripDatasets;

    @Override
    public void init(Path dataDir) {
        try (Stream<Path> paths = Files.walk(dataDir)) {
            tripDatasets = paths.filter(Files::isRegularFile)
                    .filter(path -> path.toString().endsWith(Config.PARQUET_FILE_EXTENSION))
                    .map(path -> new ParquetTripDataset(path.toString()))
                    .toList();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Map<Integer, Double> getAverageDistances(LocalDateTime start, LocalDateTime end) {
        if (start.compareTo(end) >= 0) {
            return Map.of();
        }

        var accumulator = new TripAccumulator();
        for (var dataset : tripDatasets) {
            Iterator<Trip> trips = dataset.trips(start, end);
            while (trips.hasNext()) {
                accumulator.accept(trips.next());
            }
        }
        return accumulator.getResult();
    }

    @Override
    public void close() {
        for (var tripDataset : tripDatasets) {
            try {
                tripDataset.close();
            } catch (IOException ignored) {
            }
        }
    }
}
