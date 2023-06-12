package com.github.thermo911.parquet;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.github.thermo911.AbstractTest;
import com.github.thermo911.data.Trip;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ParquetTripDatasetTest extends AbstractTest {

    @Test
    void trips() throws IOException {
        List<Trip> trips = List.of(
                newSimpleTrip(TIME_START_POINT.plusHours(1), TIME_START_POINT.plusHours(2)),
                newSimpleTrip(TIME_START_POINT.plusHours(2), TIME_START_POINT.plusHours(3)),
                newSimpleTrip(TIME_START_POINT.plusHours(3), TIME_START_POINT.plusHours(4)),
                newSimpleTrip(TIME_START_POINT.plusHours(5), TIME_START_POINT.plusHours(6))
        );

        String filePath = makeParquetFile(trips);

        LocalDateTime start = TIME_START_POINT.plusHours(2);
        LocalDateTime stop = TIME_START_POINT.plusHours(4);
        List<Trip> expectedTrips = List.of(trips.get(1), trips.get(2));

        List<Trip> actualTrips = new ArrayList<>();
        try (ParquetTripDataset parquetTripDataset = new ParquetTripDataset(filePath)) {
            Iterator<Trip> tripsIter = parquetTripDataset.trips(start, stop);
            tripsIter.forEachRemaining(actualTrips::add);
        }

        Assertions.assertEquals(expectedTrips, actualTrips);
    }

    @Test
    void trips_NoTrips_EmptyIterator() throws IOException {
        String filePath = makeParquetFile(List.of());
        try (ParquetTripDataset parquetTripDataset = new ParquetTripDataset(filePath)) {
            Iterator<Trip> tripsIter = parquetTripDataset.trips(TIME_START_POINT, TIME_START_POINT.plusHours(1));
            Assertions.assertFalse(tripsIter.hasNext());
        }
    }

    @Test
    void trips_StartAfterEnd_EmptyIterator() throws IOException {
        String filePath = makeParquetFile(List.of(newSimpleTrip(TIME_START_POINT, TIME_START_POINT.plusHours(1))));
        try (ParquetTripDataset parquetTripDataset = new ParquetTripDataset(filePath)) {
            Iterator<Trip> tripsIter = parquetTripDataset.trips(TIME_START_POINT.plusHours(1), TIME_START_POINT);
            Assertions.assertFalse(tripsIter.hasNext());
        }
    }

    private static Trip newSimpleTrip(LocalDateTime pickupTime, LocalDateTime dropoffTime) {
        return new Trip(1, 1.0, toEpochMillis(pickupTime), toEpochMillis(dropoffTime));
    }

    private static long toEpochMillis(LocalDateTime localDateTime) {
        return localDateTime.toEpochSecond(ZoneOffset.UTC) * 1_000_000L;
    }
}