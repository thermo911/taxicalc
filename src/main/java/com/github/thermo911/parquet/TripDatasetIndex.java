package com.github.thermo911.parquet;

import java.util.List;

interface TripDatasetIndex {
    /**
     * Returns indices of Parquet row groups that <i>could</i> contain searched trips.
     * @param tripStartMs number of milliseconds from the epoch start when trip was started
     * @param tripEndMs   number of milliseconds from the epoch start when trip was ended
     * @return indices of Parquet row groups
     */
    List<Integer> getRowGroupIndices(long tripStartMs, long tripEndMs);
}
