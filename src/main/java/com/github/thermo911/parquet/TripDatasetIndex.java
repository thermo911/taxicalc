package com.github.thermo911.parquet;

import java.util.List;
import java.util.stream.IntStream;

/**
 * Index structure that helps to determine which Parquet row groups
 * should be read in order to get trips fittings specified time constraints.
 */
class TripDatasetIndex {
    public record MinMax(long min, long max) {
    }

    private final List<MinMax> rowGroupMinMax;

    TripDatasetIndex(List<MinMax> rowGroupMinMax) {
        this.rowGroupMinMax = rowGroupMinMax;
    }

    /**
     * Returns indices of Parquet row groups that <i>could</i> contain searched trips.
     * @param tripStartMs number of milliseconds from the epoch start when trip was started
     * @param tripEndMs   number of milliseconds from the epoch start when trip was ended
     * @return indices of Parquet row groups
     */
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
