package com.github.thermo911;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import com.github.thermo911.data.Trip;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class AverageDistancesImplTest extends AbstractTest {

    private static final List<List<Trip>> TRIPS_DATA = List.of(
            List.of(
                    new Trip(1, 1.0, TIME_START_POINT.plusHours(0), TIME_START_POINT.plusHours(1)),
                    new Trip(2, 1.0, TIME_START_POINT.plusHours(1), TIME_START_POINT.plusHours(2)),
                    new Trip(3, 1.0, TIME_START_POINT.plusHours(2), TIME_START_POINT.plusHours(3))
            ),
            List.of(
                    new Trip(1, 2.0, TIME_START_POINT.plusHours(3), TIME_START_POINT.plusHours(4)),
                    new Trip(2, 2.0, TIME_START_POINT.plusHours(5), TIME_START_POINT.plusHours(6)),
                    new Trip(3, 2.0, TIME_START_POINT.plusHours(7), TIME_START_POINT.plusHours(8))
            ),
            List.of(
                    new Trip(1, 3.0, TIME_START_POINT.plusHours(9), TIME_START_POINT.plusHours(10)),
                    new Trip(2, 3.0, TIME_START_POINT.plusHours(11), TIME_START_POINT.plusHours(12)),
                    new Trip(3, 3.0, TIME_START_POINT.plusHours(13), TIME_START_POINT.plusHours(14))
            )
    );

    private static AverageDistancesImpl averageDistances;

    @BeforeAll
    static void setup() throws IOException {
        for (int i = 0; i < TRIPS_DATA.size(); i++) {
            makeParquetFile(TRIPS_DATA.get(i), i + ".parquet");
        }
        averageDistances = new AverageDistancesImpl();
        averageDistances.init(tempDir);
    }

    @AfterAll
    static void tearDown() {
        averageDistances.close();
    }

    @ParameterizedTest
    @MethodSource("getAverageDistancesArgs")
    void getAverageDistances(LocalDateTime start, LocalDateTime end, Map<Integer, Double> expectedResult) {
        var result = averageDistances.getAverageDistances(start, end);
        Assertions.assertEquals(expectedResult, result);
    }

    private static Stream<Arguments> getAverageDistancesArgs() {
        return Stream.of(
                Arguments.of(
                        TIME_START_POINT.minusYears(1),
                        TIME_START_POINT.plusYears(1),
                        Map.of(1, 2.0, 2, 2.0, 3, 2.0)
                ),
                Arguments.of(
                        TIME_START_POINT.plusYears(1),
                        TIME_START_POINT,
                        Map.of()
                ),
                Arguments.of(
                        TIME_START_POINT.plusHours(8),
                        TIME_START_POINT.plusHours(20),
                        Map.of(1, 3.0, 2, 3.0, 3, 3.0)
                ),
                Arguments.of(
                        TIME_START_POINT.plusHours(8),
                        TIME_START_POINT.plusHours(20),
                        Map.of(1, 3.0, 2, 3.0, 3, 3.0)
                ),
                Arguments.of(
                        TIME_START_POINT.plusHours(1),
                        TIME_START_POINT.plusHours(6),
                        Map.of(1, 2.0, 2, 1.5, 3, 1.0)
                )
        );
    }
}