package com.github.thermo911.parquet;

import java.util.List;
import java.util.stream.IntStream;

/**
 * Dummy implementation of {@link TripDatasetIndex}.
 */
class DummyTripDatasetIndex implements TripDatasetIndex {
    public record MinMax(long min, long max) {
    }

    private final List<MinMax> rowGroupMinMax;

    DummyTripDatasetIndex(List<MinMax> rowGroupMinMax) {
        this.rowGroupMinMax = rowGroupMinMax;
    }

    @Override
    public List<Integer> getRowGroupIndices(long tripStartMs, long tripEndMs) {
        return IntStream.range(0, rowGroupMinMax.size())
                .filter(i -> {
                    MinMax minMax = rowGroupMinMax.get(i);
                    return minMax.min < tripEndMs || minMax.max > tripStartMs;
                })
                .boxed()
                .toList();
    }
}
